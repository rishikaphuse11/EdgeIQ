require('dotenv').config();
const axios = require('axios');
const WebSocket = require('ws');

const MATCHMAKER_WS = process.env.MATCHMAKER_WS || 'ws://127.0.0.1:8080/lobby';
const TELEMETRY_URL = process.env.TELEMETRY_URL || 'http://127.0.0.1:8080/telemetry';
const EDGE_URLS = (process.env.EDGE_URLS || 'http://127.0.0.1:5000,http://127.0.0.1:5001,http://127.0.0.1:5002').split(',');

// 6 Distinct Regional Personas simulating diverse multi-cloud geography
const GEO_PROFILES = [
    { name: 'US-West Persona',   weights: [0.15, 0.85, 1.70] },
    { name: 'US-East Persona',   weights: [0.90, 0.18, 1.20] },
    { name: 'EU-Central Persona', weights: [1.65, 1.10, 0.20] },
    { name: 'APAC Persona',      weights: [2.10, 1.90, 1.80] },
    { name: 'LATAM Persona',     weights: [1.40, 0.65, 1.95] },
    { name: 'AU Persona',        weights: [2.40, 2.10, 1.50] }
];

function generatePingVector(rawPings, botIndex) {
    const profile = GEO_PROFILES[botIndex % GEO_PROFILES.length];
    const base = rawPings[0] && rawPings[0] < 900 ? rawPings[0] : 100;
    
    // Always output a 3-element vector corresponding to [US-West, US-East, EU-Central]
    return [0, 1, 2].map(i => {
        const measured = rawPings[i] && rawPings[i] < 900 ? rawPings[i] : (base * profile.weights[i]);
        const jitter = (Math.random() - 0.5) * 10;
        return Math.max(5, Math.round(measured + jitter));
    });
}

async function measurePing(url) {
    const start = Date.now();
    try {
        await axios.get(`${url.trim()}/ping`, { timeout: 1500 });
        return Date.now() - start;
    } catch (e) {
        return 999;
    }
}

function pickAction() {
    const roll = Math.random();
    if (roll < 0.35) return 'left';
    if (roll < 0.70) return 'right';
    if (roll < 0.85) return 'jump';
    if (roll < 0.92) return 'shoot';
    if (roll < 0.97) return 'shield';
    return null;
}

async function bootAgent(clientId, botIndex) {
    const rawPings = await Promise.all(EDGE_URLS.map(measurePing));
    const pings = generatePingVector(rawPings, botIndex);
    const profile = GEO_PROFILES[botIndex % GEO_PROFILES.length];
    console.log(`[${clientId}] Profile (${profile.name}) -> Pings:[${pings.join('ms, ')}ms]`);
    
    let mmWs = null;
    const connectToMatchmaker = () => {
        try {
            mmWs = new WebSocket(MATCHMAKER_WS);
            
            mmWs.on('open', () => {
                mmWs.send(JSON.stringify({ client_id: clientId, pings: pings }));
            });
            
            mmWs.on('message', (data) => {
                const payload = JSON.parse(data);
                if (payload.status === 'match_found') {
                    console.log(`[${clientId}] Match Found! Connecting to ${payload.server_ip}:${payload.server_port}`);
                    mmWs.close();
                    
                    const gameWs = new WebSocket(`ws://${payload.server_ip}:${payload.server_port}`);
                    let actionInterval = null;

                    gameWs.on('open', () => {
                        gameWs.send(JSON.stringify({ type: 'auth', token: payload.token }));
                        let lastPingTime = 0;
                        
                        gameWs.on('message', (msg) => {
                            const data = JSON.parse(msg);
                            if (data.type === 'pong') {
                                const rtt = Date.now() - lastPingTime;
                                axios.post(TELEMETRY_URL, { client_id: clientId, actual_rtt: rtt }).catch(() => {});
                            }
                        });

                        actionInterval = setInterval(() => {
                            if (gameWs.readyState === WebSocket.OPEN) {
                                const action = pickAction();
                                if (action) gameWs.send(JSON.stringify({ type: 'input', action }));
                                if (Math.random() < 0.1) {
                                    lastPingTime = Date.now();
                                    gameWs.send(JSON.stringify({ type: 'ping' }));
                                }
                            }
                        }, 250);
                    });

                    gameWs.on('close', () => {
                        if (actionInterval) clearInterval(actionInterval);
                        setTimeout(connectToMatchmaker, 2000);
                    });

                    gameWs.on('error', () => {
                        if (actionInterval) clearInterval(actionInterval);
                        setTimeout(connectToMatchmaker, 2000);
                    });
                }
            });

            mmWs.on('error', () => {
                setTimeout(connectToMatchmaker, 2000);
            });
        } catch (e) {
            setTimeout(connectToMatchmaker, 2000);
        }
    };

    connectToMatchmaker();
}

const NUM_BOTS = parseInt(process.env.NUM_BOTS || '60', 10);
console.log(`[Swarm] Booting ${NUM_BOTS} bots with 6 regional personas...`);
for (let i = 0; i < NUM_BOTS; i++) {
    bootAgent(`bot_${i}`, i);
}
