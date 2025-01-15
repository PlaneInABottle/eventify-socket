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

const expressMiddleware = (req, res, next) => {
  try {
    // Get the auth header
    const authHeader = req.headers.authorization;

    // Check if auth header exists
    if (!authHeader) {
      return res.status(401).json({ error: "No authorization header" });
    }

    // Check if it's a Bearer token
    if (!authHeader.startsWith("Bearer ")) {
      return res.status(401).json({ error: "Invalid token format" });
    }

    // Extract the token
    const token = authHeader.split(" ")[1];

    // Use the hashed secret for verification
    const jwtSecret = process.env.JWT_SECRET;
    const decoded = jwt.verify(token, jwtSecret);

    // Filter out iat, exp, and collection from decoded data
    const { iat, exp, collection, ...userData } = decoded;
    req.user = userData;

    next();
  } catch (error) {
    if (error instanceof jwt.TokenExpiredError) {
      return res.status(401).json({ error: "Token expired" });
    }
    if (error instanceof jwt.JsonWebTokenError) {
      return res.status(401).json({ error: "Invalid token" });
    }

    return res.status(500).json({ error: "Authentication failed" });
  }
};

// Middleware to parse JSON bodies
app.use(express.json());

app.post("/api/broadcast", expressMiddleware, async (req, res) => {
  // Add sender information from the JWT token
  const messageData = {
    ...req.body,
    sender: req.user, // This includes the user info from JWT token
  };
  // await messageQueue.add(messageData);
  // console.log("Message queued");
  // res.status(202).json({ queued: true });

  const { event, data, room, sender } = messageData;
  // console.log(event, data, room, sender);
  if (room) {
    io.to(room.toString()).emit(event, { data, sender, room });
  } else {
    io.emit(event, { data, sender });
  }
  res.status(200).end();
});

// Process queue separately
messageQueue.process(async (job) => {
  try {
    const { event, data, room, sender } = job.data;
    if (room) {
      io.to(room).emit(event, { data, sender });
    } else {
      io.emit(event, { data, sender });
    }
  } catch (error) {
    console.error("Error processing message:", error);
    throw error; // Allows Bull to handle the failure
  }
});

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

    // Use the hashed secret for verification
    const jwtSecret = process.env.JWT_SECRET;
    const decoded = jwt.verify(cleanToken, jwtSecret);

    if (!decoded) {
      return next(new Error("Authentication error: Token verification failed"));
    }

    // Filter out iat, exp, and collection from decoded data
    const { iat, exp, collection, ...userData } = decoded;
    socket.user = userData;
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
    console.log(`User ${socket.user.id} joined room: ${roomName}`);
  });

  // Leave room with validation
  socket.on("leaveRoom", (roomName) => {
    if (typeof roomName !== "string" || !roomName.trim()) {
      socket.emit("error", { message: "Invalid room name" });
      return;
    }
    socket.leave(roomName);
    console.log(`User ${socket.user.id} left room: ${roomName}`);
  });

  // Handle sending messages to a room
  // socket.on("sendMessage", ({ roomName, message }) => {
  // 	console.log(`Message: ${message}, Room: ${roomName}`);
  // 	io.to(roomName).emit("receiveMessage", message); // Emit message to the room
  // });

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
