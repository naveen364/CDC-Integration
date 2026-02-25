'use strict';

const axios = require('axios');

const WEBHOOK_URL = process.env.WEBHOOK_URL || '';

/**
 * handle() — receives a raw CDC event from Salesforce Streaming API
 * and returns a normalised event object.
 *
 * Salesforce CDC event structure:
 * {
 *   schema: "<schemaId>",
 *   payload: {
 *     ChangeEventHeader: {
 *       entityName: "Contact",
 *       changeType: "UPDATE",        // CREATE | UPDATE | DELETE | UNDELETE
 *       recordIds: ["003..."],
 *       changedFields: ["Phone","Title"],
 *       changeOrigin: "...",
 *       transactionKey: "...",
 *       sequenceNumber: 1,
 *       commitTimestamp: 1700000000000,
 *       commitNumber: 123456,
 *       commitUser: "005..."
 *     },
 *     // changed field values are at the top level of payload:
 *     Phone: "+91-9876543210",
 *     Title: "Manager"
 *   },
 *   event: { replayId: 42 }
 * }
 */
function handle(rawEvent) {
    const header  = rawEvent.payload?.ChangeEventHeader || {};
    const replayId = rawEvent.event?.replayId;

    // Extract only the changed field values (everything except ChangeEventHeader)
    const changedValues = {};
    for (const [key, value] of Object.entries(rawEvent.payload || {})) {
        if (key !== 'ChangeEventHeader') {
            changedValues[key] = value;
        }
    }

    const normalised = {
        replayId,
        receivedAt       : new Date().toISOString(),
        entityName       : header.entityName,
        changeType       : header.changeType,          // CREATE | UPDATE | DELETE | UNDELETE
        recordIds        : header.recordIds || [],
        changedFields    : header.changedFields || [],
        changedValues,
        commitTimestamp  : header.commitTimestamp
            ? new Date(header.commitTimestamp).toISOString()
            : null,
        commitUser       : header.commitUser,
        transactionKey   : header.transactionKey,
        changeOrigin     : header.changeOrigin,
    };

    // Pretty-print to console
    console.log('─'.repeat(60));
    console.log(`📬  CDC Event Received`);
    console.log(`    Entity      : ${normalised.entityName}`);
    console.log(`    Change Type : ${normalised.changeType}`);
    console.log(`    Record IDs  : ${normalised.recordIds.join(', ')}`);
    console.log(`    Fields      : ${normalised.changedFields.join(', ')}`);
    console.log(`    Values      :`, changedValues);
    console.log(`    Commit Time : ${normalised.commitTimestamp}`);
    console.log(`    Replay ID   : ${replayId}`);
    console.log('─'.repeat(60));

    // ── Optional webhook forwarding ───────────────────────────────────────────
    if (WEBHOOK_URL) {
        axios.post(WEBHOOK_URL, normalised)
            .then(() => console.log(`   ↗  Forwarded to webhook: ${WEBHOOK_URL}`))
            .catch((err) => console.error(`   ⚠  Webhook forward failed: ${err.message}`));
    }

    return normalised;
}

module.exports = { handle };
