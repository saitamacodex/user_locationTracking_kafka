import http from "node:http";
import express from "express";
import { Server } from "socket.io";
import path from "node:path";

async function main() {
  const PORT = process.env.PORT ?? 9090;
  const app = express();
  const server = http.createServer(app);
  const io = new Server();

  io.attach(server);
  app.use(express.static(path.resolve("./public")));

  app.get("/health", (req, res) => {
    return res.json({ healthy: "OK" });
  });

  // socket hander
  io.on("connection", (socket) => {
    console.log(`socket connected ${socket.id}`);

    socket.on("client:location:update", (locationData) => {
      const { latitude, longitude } = locationData;
      console.log(latitude, longitude);
    });
  });

  server.listen(PORT, () => {
    console.log(`server running on http://localhost:${PORT}`);
  });
}

main();
