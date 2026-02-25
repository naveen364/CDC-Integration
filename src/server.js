'use strict';

require('dotenv').config();
const express           = require('express');
const cors              = require('cors');
const { startSubscriber, eventStore } = require('./subscriber');

const PORT = parseInt(process.env.PORT || '3000', 10);
const app  = express();

app.use(cors());
app.use(express.json());

// ── Routes ───────────────────────────────────────────────────────────────────

/**
 * GET /health
 * Returns server health status.
 */
app.get('/health', (_req, res) => {
    res.json({
        status    : 'ok',
        service   : 'Salesforce CDC Node Server',
        timestamp : new Date().toISOString(),
        channel   : process.env.CDC_CHANNEL || '/data/ContactChangeEvent',
    });
});

/**
 * GET /events
 * Returns cached CDC events (latest first).
 * Query params:
 *   ?limit=20      — max number of events to return (default: 20)
 *   ?changeType=UPDATE — filter by changeType (CREATE | UPDATE | DELETE | UNDELETE)
 *   ?entity=Contact    — filter by entityName
 */
app.get('/events', (req, res) => {
    let results = [...eventStore];

    const { limit = 20, changeType, entity } = req.query;

    if (changeType) {
        results = results.filter(
            (e) => e.changeType?.toUpperCase() === changeType.toUpperCase()
        );
    }

    if (entity) {
        results = results.filter(
            (e) => e.entityName?.toLowerCase() === entity.toLowerCase()
        );
    }

    res.json({
        total   : results.length,
        events  : results.slice(0, parseInt(limit, 10)),
    });
});

/**
 * GET /events/:replayId
 * Returns a single event by replayId.
 */
app.get('/events/:replayId', (req, res) => {
    const id    = parseInt(req.params.replayId, 10);
    const event = eventStore.find((e) => e.replayId === id);

    if (!event) {
        return res.status(404).json({ error: `No event with replayId ${id}` });
    }
    res.json(event);
});

/**
 * DELETE /events
 * Clears the in-memory event store.
 */
app.delete('/events', (_req, res) => {
    eventStore.length = 0;
    res.json({ message: 'Event store cleared.' });
});

// ── 404 Handler ───────────────────────────────────────────────────────────────
app.use((_req, res) => {
    res.status(404).json({ error: 'Not found' });
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
    console.log(`\n🚀  CDC Node Server running on http://localhost:${PORT}`);
    console.log(`    GET  /health          — health check`);
    console.log(`    GET  /events          — list cached events`);
    console.log(`    GET  /events/:id      — get event by replayId`);
    console.log(`    DELETE /events        — clear event cache\n`);

    // Start the Salesforce CDC subscriber automatically
    startSubscriber().catch((err) => {
        console.error('❌  Subscriber failed to start:', err.message);
    });
});
