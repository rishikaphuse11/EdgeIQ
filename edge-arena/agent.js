require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { spawn } = require('child_process');
const crypto = require('crypto');
const net = require('net');

const app = express();
app.use(cors());
app.use(express.json());

const AGENT_PORT = process.env.PORT || 5000;
let activePorts = new Set();

function isPortAvailable(port) {
    return new Promise((resolve) => {
        const server = net.createServer();
        server.once('error', () => resolve(false));
        server.once('listening', () => {
            server.close(() => resolve(true));
        });
        server.listen(port);
    });
}

async function findAvailablePort(startPort = 7000, endPort = 7100) {
    for (let port = startPort; port <= endPort; port++) {
        if (!activePorts.has(port) && await isPortAvailable(port)) {
            return port;
        }
    }
    throw new Error('No available ports in range');
}

// TCP readiness probe: polls until child process TCP port accepts connections
function waitForPort(port, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
        const deadline = Date.now() + timeoutMs;
        
        function tryConnect() {
            if (Date.now() > deadline) {
                return reject(new Error(`Port ${port} not ready within ${timeoutMs}ms`));
            }
            
            const socket = new net.Socket();
            socket.once('connect', () => {
                socket.destroy();
                resolve();
            });
            socket.once('error', () => {
                socket.destroy();
                setTimeout(tryConnect, 100);
            });
            socket.connect(port, '127.0.0.1');
        }
        
        tryConnect();
    });
}

app.get('/ping', (req, res) => {
    const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const hash = crypto.createHash('md5').update(clientIp).digest('hex');
    const baseLatency = parseInt(hash.substring(0, 4), 16) % 100;
    const jitter = Math.random() < 0.1 ? Math.random() * 200 : Math.random() * 20;

    setTimeout(() => {
        res.status(200).json({ status: 'ok', injected_delay: baseLatency + jitter });
    }, baseLatency + jitter);
});

// Authenticated endpoint to spawn game servers with TCP readiness gate
app.post('/provision', async (req, res) => {
    const authHeader = req.headers['authorization'];
    if (authHeader !== `Bearer ${process.env.AGENT_SECRET || 'super_secret_agent_key'}`) {
        return res.sendStatus(403);
    }
    
    try {
        const gamePort = await findAvailablePort();
        activePorts.add(gamePort);

        console.log(`[Agent] Provisioning new arena on port ${gamePort}`);
        const gameProcess = spawn('node', ['server.js'], {
            env: { ...process.env, PORT: gamePort }
        });

        gameProcess.stdout.on('data', (data) => console.log(`[Arena-${gamePort}] ${data}`));
        
        gameProcess.on('exit', () => {
            console.log(`[Agent] Arena on port ${gamePort} terminated. Freeing port.`);
            activePorts.delete(gamePort);
        });

        // Readiness probe ensures game server port is listening before returning 200
        await waitForPort(gamePort, 5000);
        res.json({ status: 'provisioned', port: gamePort });
    } catch (err) {
        res.status(503).json({ error: err.message });
    }
});

app.listen(AGENT_PORT, () => console.log(`Edge Agent running on port ${AGENT_PORT}`));
