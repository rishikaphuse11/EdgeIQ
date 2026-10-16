require('dotenv').config();
const WebSocket = require('ws');
const jwt = require('jsonwebtoken');

const PORT = process.env.PORT || 7000;
const JWT_SECRET = process.env.JWT_SECRET || 'super_secret_key';
const TICK_RATE = 20;
const TICK_MS = 1000 / TICK_RATE;
const MATCH_DURATION = 90;

const server = new WebSocket.Server({ port: PORT });
let players = {};
let projectiles = [];
let nextProjectileId = 1;
let matchTimeRemaining = MATCH_DURATION;
let matchStarted = false;
let totalJoinedPlayers = 0;

const platforms = [
    { x: 100, y: 100, width: 200, height: 10 },
    { x: 450, y: 180, width: 200, height: 10 },
    { x: 250, y: 260, width: 300, height: 10 }
];

const powerups = [
    { id: 'p1', x: 200, y: 120, type: 'shield' },
    { id: 'p2', x: 550, y: 200, type: 'shield' }
];

server.on('connection', (ws, req) => {
    const isSpectator = req.url.includes('spectate=true');
    let playerId = null;
    let isAuthenticated = isSpectator;

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            
            if (!isAuthenticated) {
                if (data.type === 'auth' && data.token) {
                    jwt.verify(data.token, JWT_SECRET, (err, decoded) => {
                        if (err) return ws.close(4001, 'Unauthorized');
                        isAuthenticated = true;
                        playerId = decoded.client_id;
                        totalJoinedPlayers++;
                        players[playerId] = { 
                            x: Math.floor(Math.random() * 600) + 100, 
                            y: 300, 
                            vx: 0, 
                            vy: 0, 
                            facing: 1,
                            shield: false, 
                            kills: 0,
                            lastShotTime: 0,
                            color: `hsl(${Math.floor(Math.random() * 360)}, 80%, 60%)`,
                            ws
                        };
                        ws.send(JSON.stringify({ type: 'authenticated', client_id: playerId }));
                        if (!matchStarted) matchStarted = true;
                    });
                } else {
                    ws.close(4001, 'Auth payload required first');
                }
                return;
            }

            if (data.type === 'ping') return ws.send(JSON.stringify({ type: 'pong' }));
            
            if (data.type === 'input' && playerId && players[playerId]) {
                const p = players[playerId];
                if (data.action === 'left') { p.vx = -150; p.facing = -1; }
                if (data.action === 'right') { p.vx = 150; p.facing = 1; }
                if (data.action === 'jump' && (p.y <= 0.1 || p.onPlatform)) p.vy = 280;
                if (data.action === 'shield') p.shield = !p.shield;
                if (data.action === 'shoot') {
                    const now = Date.now();
                    if (now - p.lastShotTime >= 800) {
                        p.lastShotTime = now;
                        projectiles.push({
                            id: nextProjectileId++,
                            x: p.x + (p.facing === 1 ? 35 : -10),
                            y: p.y + 15,
                            vx: p.facing * 450,
                            ownerId: playerId,
                            ttl: 2.0
                        });
                    }
                }
            }
        } catch (e) {
            console.error('Invalid message format');
        }
    });

    ws.on('close', () => {
        if (playerId && players[playerId]) {
            delete players[playerId];
            const remaining = Object.keys(players);
            if (remaining.length === 0) {
                console.log(`[Arena-${PORT}] Empty lobby. Gracefully shutting down.`);
                process.exit(0);
            } else if (remaining.length === 1 && totalJoinedPlayers > 1) {
                endMatch(`${remaining[0]} wins! Last player standing.`);
            }
        }
    });
});

setInterval(() => {
    if (matchStarted && matchTimeRemaining > 0) {
        matchTimeRemaining--;
        if (matchTimeRemaining <= 0) {
            endMatch('Timer expired');
        }
    }
}, 1000);

function endMatch(reason) {
    console.log(`[Arena-${PORT}] Match ended: ${reason}`);
    const scoreboard = Object.entries(players).map(([id, p]) => ({ id, kills: p.kills }));
    const payload = JSON.stringify({ type: 'game_over', reason, scoreboard });
    
    server.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(payload);
    });
    
    setTimeout(() => process.exit(0), 1000);
}

// Physics loop handling projectiles, collisions, and last-man-standing check
let lastTime = process.hrtime.bigint();
setInterval(() => {
    const now = process.hrtime.bigint();
    const dt = Number(now - lastTime) / 1e9;
    lastTime = now;

    for (let i = projectiles.length - 1; i >= 0; i--) {
        const proj = projectiles[i];
        proj.x += proj.vx * dt;
        proj.ttl -= dt;

        if (proj.ttl <= 0 || proj.x < 0 || proj.x > 800) {
            projectiles.splice(i, 1);
            continue;
        }

        for (const targetId in players) {
            if (targetId === proj.ownerId) continue;
            const target = players[targetId];
            if (Math.abs(proj.x - target.x) < 25 && Math.abs(proj.y - target.y) < 30) {
                if (target.shield) {
                    target.shield = false;
                } else {
                    if (players[proj.ownerId]) players[proj.ownerId].kills += 1;
                    if (target.ws && target.ws.readyState === WebSocket.OPEN) {
                        target.ws.send(JSON.stringify({ type: 'eliminated' }));
                    }
                    delete players[targetId];

                    const remaining = Object.keys(players);
                    if (remaining.length === 1 && totalJoinedPlayers > 1) {
                        endMatch(`${remaining[0]} wins! Last player standing.`);
                        return;
                    }
                }
                projectiles.splice(i, 1);
                break;
            }
        }
    }

    const serializablePlayers = {};
    for (const id in players) {
        let p = players[id];
        p.vy -= 450 * dt;
        p.x += p.vx * dt;
        p.y += p.vy * dt;
        p.onPlatform = false;

        if (p.y < 0) { p.y = 0; p.vy = 0; }
        if (p.x < 50) p.x = 50;
        if (p.x > 750) p.x = 750;

        for (const plat of platforms) {
            if (p.x + 30 >= plat.x && p.x <= plat.x + plat.width && p.vy <= 0) {
                if (p.y <= plat.y + plat.height && p.y >= plat.y) {
                    p.y = plat.y + plat.height;
                    p.vy = 0;
                    p.onPlatform = true;
                }
            }
        }
        p.vx *= 0.85;

        serializablePlayers[id] = {
            x: Math.round(p.x),
            y: Math.round(p.y),
            facing: p.facing,
            shield: p.shield,
            kills: p.kills,
            color: p.color
        };
    }

    const deltaState = {
        players: serializablePlayers,
        projectiles: projectiles.map(p => ({ x: Math.round(p.x), y: Math.round(p.y) })),
        platforms,
        powerups,
        time_remaining: matchTimeRemaining
    };

    const payload = JSON.stringify({ type: 'delta_state', state: deltaState, dt });
    server.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(payload);
    });
}, TICK_MS);

console.log(`[Arena-${PORT}] Booted with last-man-standing logic and shoot cooldown (800ms).`);
