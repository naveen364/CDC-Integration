# Salesforce CDC → Node.js External Server

Listen to **Salesforce Change Data Capture (CDC)** events in real-time and expose them via a REST API.

---

## Architecture

```
Salesforce Org
   │  (CDC streaming channel)
   ▼
jsforce Subscriber  (subscriber.js)
   │  (normalised event)
   ▼
Event Handler       (eventHandler.js)  ──► Webhook (optional)
   │  (in-memory store)
   ▼
Express REST API    (server.js)        ──► External Systems
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| Node.js | v18 or later |
| Salesforce Org | Developer / Sandbox / Production |
| CDC Enabled | Setup → Integrations → Change Data Capture |
| Streaming API | Enabled (default in all orgs) |

---

## Salesforce Setup

### 1. Enable CDC for Contact

In your Salesforce Org:

1. Go to **Setup → Integrations → Change Data Capture**
2. Move **Contact** from *Available Entities* to *Selected Entities*
3. Click **Save**

---

## Node.js Server Setup

### 1. Install dependencies

```bash
cd cdc-node-server
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your Salesforce credentials:

```env
SF_USERNAME=your@org.com
SF_PASSWORD=yourPassword
SF_SECURITY_TOKEN=yourToken
SF_LOGIN_URL=https://login.salesforce.com
CDC_CHANNEL=/data/ContactChangeEvent
REPLAY_ID=-1
PORT=3000
WEBHOOK_URL=          # optional — leave blank to disable
```

> **Where to find your Security Token:**  
> Salesforce → Profile icon → Settings → My Personal Information → Reset My Security Token

### 3. Start the server

```bash
npm start
```

The server will:
1. Start the Express API on `http://localhost:3000`
2. Authenticate to Salesforce
3. Subscribe to the CDC channel
4. Print every incoming change event to the console

---

## REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/events` | List cached events (newest first) |
| `GET` | `/events?limit=10` | Limit results |
| `GET` | `/events?changeType=UPDATE` | Filter by change type |
| `GET` | `/events?entity=Contact` | Filter by entity |
| `GET` | `/events/:replayId` | Get single event by replay ID |
| `DELETE` | `/events` | Clear event cache |

### Example Response — `GET /events`

```json
{
  "total": 1,
  "events": [
    {
      "replayId": 42,
      "receivedAt": "2026-02-25T08:40:00.000Z",
      "entityName": "Contact",
      "changeType": "UPDATE",
      "recordIds": ["0034X00000AbCdEQAT"],
      "changedFields": ["Phone", "Title"],
      "changedValues": {
        "Phone": "+91-9876543210",
        "Title": "Senior Manager"
      },
      "commitTimestamp": "2026-02-25T08:39:58.000Z",
      "commitUser": "0054X00000XyZaQAA"
    }
  ]
}
```

---

## Supported CDC Channels

Change the `CDC_CHANNEL` in `.env` to track a different object:

| Object | Channel |
|---|---|
| Contact | `/data/ContactChangeEvent` |
| Account | `/data/AccountChangeEvent` |
| Lead | `/data/LeadChangeEvent` |
| Opportunity | `/data/OpportunityChangeEvent` |
| Any Custom Object | `/data/MyObject__ChangeEvent` |

---

## Change Types

| changeType | Meaning |
|---|---|
| `CREATE` | New record created |
| `UPDATE` | Existing record updated |
| `DELETE` | Record deleted |
| `UNDELETE` | Record restored from Recycle Bin |
| `GAP_CREATE/UPDATE/DELETE` | Events missed during downtime |

---

## Project Structure

```
cdc-node-server/
├── src/
│   ├── server.js        ← Express REST API + boots subscriber
│   ├── subscriber.js    ← Salesforce streaming client (jsforce)
│   └── eventHandler.js  ← Normalises & logs CDC events
├── .env.example         ← Environment template
├── .gitignore
└── package.json
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Login failed` | Check SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN |
| `Handshake failed` | Check Streaming API is enabled in your org |
| No events received | Confirm CDC is enabled for the entity in Setup |
| `INVALID_TYPE` error | Verify the CDC channel name matches your object |

