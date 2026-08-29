import compress from "@fastify/compress";
import cors from "@fastify/cors";
import Fastify, { type FastifyInstance } from "fastify";
import { config } from "../config.js";
import { log, logErr } from "../log.js";
import { registerRoutes } from "./routes.js";

export async function startApi(): Promise<FastifyInstance> {
  const app = Fastify({ logger: false, trustProxy: true });
  await app.register(cors, { origin: "*" });
  // JSON time series compresses ~10x, and Railway bills egress per GB. gzip only:
  // brotli buys little more on this payload shape and costs real CPU.
  await app.register(compress, { encodings: ["gzip", "deflate"] });

  app.setErrorHandler((err, req, reply) => {
    logErr("api", `${req.method} ${req.url} failed`, err);
    void reply.code(500).send({ error: { code: "internal_error", message: "internal error" } });
  });
  app.setNotFoundHandler((req, reply) => {
    void reply.code(404).send({ error: { code: "not_found", message: `no route: ${req.method} ${req.url}` } });
  });

  registerRoutes(app);
  await app.listen({ port: config.port, host: config.host });
  log("api", `listening on ${config.host}:${config.port}`);
  return app;
}
