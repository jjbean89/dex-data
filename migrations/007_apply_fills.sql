-- Applies an ordered sequence of fills to one wallet's position in a coin,
-- maintaining szi AND the average entry price:
--   from flat            -> entry = fill price
--   adding (same side)   -> entry = size-weighted blend (unknown legacy entry stays unknown)
--   reducing             -> entry unchanged
--   closing to flat      -> entry cleared
--   flipping through 0   -> entry = the flipping fill's price
-- The row is locked FOR UPDATE for the transaction, serializing fills against the
-- clearinghouseState verifier's delete+insert. Sub-1e-9 residues count as flat
-- (fill sizes are decimal strings; float sums can leave dust).
create or replace function apply_fills(p_address text, p_coin text, p_fills jsonb)
returns void
language plpgsql
as $$
declare
  v_szi   double precision;
  v_entry double precision;
  v_fill  jsonb;
  f_sz    double precision;
  f_px    double precision;
  v_new   double precision;
begin
  select szi, entry_px into v_szi, v_entry
  from positions
  where address = p_address and coin = p_coin
  for update;
  if not found then
    v_szi := 0;
    v_entry := null;
  end if;

  for v_fill in select * from jsonb_array_elements(p_fills) loop
    f_sz := (v_fill->>'sz')::double precision;
    f_px := (v_fill->>'px')::double precision;
    if f_sz is null or f_px is null or f_sz = 0 then
      continue;
    end if;

    if abs(v_szi) < 1e-9 then
      v_szi := f_sz;
      v_entry := f_px;
    elsif (f_sz > 0) = (v_szi > 0) then
      if v_entry is not null then
        v_entry := (v_entry * abs(v_szi) + f_px * abs(f_sz)) / (abs(v_szi) + abs(f_sz));
      end if;
      v_szi := v_szi + f_sz;
    else
      v_new := v_szi + f_sz;
      if abs(v_new) < 1e-9 then
        v_szi := 0;
        v_entry := null;
      elsif (v_new > 0) = (v_szi > 0) then
        v_szi := v_new;
      else
        v_szi := v_new;
        v_entry := f_px;
      end if;
    end if;
  end loop;

  insert into positions (address, coin, szi, entry_px, updated_at)
  values (p_address, p_coin, v_szi, v_entry, now())
  on conflict (address, coin) do update
    set szi = excluded.szi, entry_px = excluded.entry_px, updated_at = now();
end;
$$;
