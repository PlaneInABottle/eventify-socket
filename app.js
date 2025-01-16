const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const jwt = require("jsonwebtoken");
const dotenv = require("dotenv");
dotenv.config();

const Queue = require("bull");
const messageQueue = new Queue("socket-messages");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Check for required environment variables
if (!process.env.JWT_SECRET) {
  console.error("JWT_SECRET environment variable is not set");
  process.exit(1);
}

// Middleware to parse JSON bodies
app.use(express.json());

// Add this middleware before your io.on("connection") setup
io.use((socket, next) => {
  try {
    const token =
      socket.handshake.auth.token || socket.handshake.headers.authorization;

    if (!token) {
      return next(new Error("Authentication error: Token required"));
    }

    // Remove 'Bearer ' if present
    const cleanToken = token.replace("Bearer ", "");

    if (!process.env.JWT_SECRET) throw Error("JWT Secret not found");
    // Use the hashed secret for verification
    const jwtSecret = process.env.JWT_SECRET;

    const decoded = jwt.verify(cleanToken, jwtSecret);

    if (!decoded) {
      return next(new Error("Authentication error: Token verification failed"));
    }

    // Filter out iat, exp, and collection from decoded data
    const {
      iat,
      exp,
      collection,
      user_role,
      user_metadata: { display_name },
      email,
      sub,
    } = decoded;
    socket.user = { display_name, email, sub, user_role };
    next();
  } catch (error) {
    console.error("JWT Verification Error:", error);
    next(new Error(`Authentication error: ${error.message}`));
  }
});

// Socket.IO logic
io.on("connection", (socket) => {
  console.log("A user connected:", socket.user);

  // Join a room with validation
  socket.on("joinRoom", (roomName) => {
    if (typeof roomName !== "string" || !roomName.trim()) {
      socket.emit("error", { message: "Invalid room name" });
      return;
    }
    socket.join(roomName);
    console.log(`User ${socket.user.sub} joined room: ${roomName}`);
  });

  socket.on("send-message", (message) => {
    console.log("New message arrived to the socket: ", message);
    io.to(message.event_chat_id).emit("get-message", {
      id: message.id,
      content: message.content,
      sender: {
        id: socket.user.sub,
        display_name: socket.user.display_name,
        user_role: socket.user.user_role,
      },
      event_chat_id: message.event_chat_id,
    });
  });

  // Leave room with validation
  socket.on("leaveRoom", (roomName) => {
    if (typeof roomName !== "string" || !roomName.trim()) {
      socket.emit("error", { message: "Invalid room name" });
      return;
    }
    socket.leave(roomName);
    console.log(`User ${socket.user.sub} left room: ${roomName}`);
  });

  // When the user disconnects, they automatically leave any rooms they joined
  socket.on("disconnect", () => {
    console.log("User disconnected");
    // Any rooms the user was in are automatically left by the server
  });
});

// Start the server
const PORT = 3001;
server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
