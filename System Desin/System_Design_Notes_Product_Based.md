# System Design Notes
### Zomato | Uber | WhatsApp | Netflix
*(High-Level Design + Low-Level Design with Flowcharts)*

---

## 1. ZOMATO (Food Delivery Platform)

### 1.1 Requirements

**Functional**
- Search restaurants by location/cuisine
- View menu, place order, make payment
- Real-time order tracking
- Delivery partner assignment
- Ratings & reviews

**Non-Functional**
- Low latency search (< 200ms)
- High availability (99.9%)
- Consistency for payments (strong), eventual consistency for search/catalog
- Scalable to millions of concurrent users

### 1.2 High-Level Design (HLD)

```mermaid
flowchart TD
    Client[Mobile / Web Client] --> LB[Load Balancer]
    LB --> API[API Gateway]
    API --> Auth[Auth Service]
    API --> Search[Search Service]
    API --> Order[Order Service]
    API --> Restaurant[Restaurant Service]
    API --> Delivery[Delivery/Dispatch Service]
    API --> Payment[Payment Service]
    API --> Notif[Notification Service]

    Search --> ES[(Elasticsearch)]
    Restaurant --> RDB[(Restaurant DB - SQL)]
    Order --> ODB[(Order DB - SQL/NoSQL)]
    Delivery --> Geo[(Geo-Location Service / Redis GEO)]
    Payment --> PG[Payment Gateway]
    Notif --> Push[Push/SMS/Email Providers]

    Order --> Kafka[(Kafka Event Bus)]
    Kafka --> Delivery
    Kafka --> Notif
    Kafka --> Analytics[Analytics/Data Warehouse]

    Delivery --> Rider[Delivery Partner App]
```

### 1.3 Low-Level Design (LLD) — Order Placement Flow

```mermaid
flowchart TD
    A[User selects items and clicks Checkout] --> B{Cart validation: items available and prices current}
    B -- Invalid --> C[Return error, refresh cart]
    B -- Valid --> D[Create Order: status = PENDING]
    D --> E[Call Payment Service]
    E --> F{Payment success?}
    F -- No --> G[Order status = FAILED, notify user]
    F -- Yes --> H[Order status = CONFIRMED]
    H --> I[Publish OrderConfirmed event to Kafka]
    I --> J[Dispatch Service finds nearest available delivery partner]
    J --> K[Assign partner, Order status = ASSIGNED]
    K --> L[Notify restaurant to start preparing]
    L --> M[Partner picks up order, status = PICKED_UP]
    M --> N[Live location updates via WebSocket/Redis GEO]
    N --> O[Order delivered, status = DELIVERED]
    O --> P[Trigger rating/review prompt]
```

### 1.4 Key Design Points
- **Database**: SQL (PostgreSQL/MySQL) for orders/payments (ACID needed); NoSQL (Cassandra) for high-write logs
- **Search**: Elasticsearch for restaurant/menu search with geo-filtering
- **Dispatch algorithm**: nearest-partner matching using geohashing (Redis GEO or Uber's H3)
- **Caching**: Redis for restaurant menus, popular searches
- **Event-driven**: Kafka decouples order → dispatch → notification → analytics

---

## 2. UBER (Ride-Hailing Platform)

### 2.1 Requirements

**Functional**
- Rider requests ride, driver accepts
- Real-time GPS tracking & ETA
- Dynamic (surge) pricing
- Trip billing & payment

**Non-Functional**
- Extremely low-latency matching (< 1s)
- High availability, partition tolerance (AP over CP for location data)
- Massive concurrent write throughput (location pings every few seconds per driver)

### 2.2 High-Level Design (HLD)

```mermaid
flowchart TD
    Rider[Rider App] --> LB[Load Balancer / API Gateway]
    Driver[Driver App] --> LB
    LB --> MatchSvc[Matching Service]
    LB --> TripSvc[Trip Service]
    LB --> PricingSvc[Pricing/Surge Service]
    LB --> LocationSvc[Location Ingestion Service]

    LocationSvc --> GeoIndex[(Geo-Spatial Index - Redis/H3)]
    MatchSvc --> GeoIndex
    MatchSvc --> DriverState[(Driver State Store - in-memory)]
    TripSvc --> TripDB[(Trip DB - SQL)]
    PricingSvc --> DemandData[(Real-time Demand/Supply Stream)]

    TripSvc --> Payment[Payment Service]
    TripSvc --> Kafka[(Kafka Event Stream)]
    Kafka --> Analytics[Analytics / ML Pricing Models]
    Kafka --> Notif[Notification Service]
```

### 2.3 Low-Level Design (LLD) — Ride Matching Flow

```mermaid
flowchart TD
    A[Rider requests ride with pickup location] --> B[Location Service geocodes pickup point]
    B --> C[Query Geo-Index for nearby available drivers within radius]
    C --> D{Drivers found?}
    D -- No --> E[Expand search radius, retry]
    D -- Yes --> F[Rank drivers by ETA, rating, acceptance rate]
    F --> G[Send ride request to top driver]
    G --> H{Driver accepts within timeout?}
    H -- No --> I[Move to next ranked driver]
    H -- Yes --> J[Lock driver, create Trip record status=ACCEPTED]
    J --> K[Notify rider with driver details and ETA]
    K --> L[Driver navigates to pickup, live location streamed]
    L --> M[Trip starts, status=IN_PROGRESS]
    M --> N[Continuous fare calculation based on distance/time/surge]
    N --> O[Trip ends, status=COMPLETED]
    O --> P[Calculate final fare, charge payment method]
    P --> Q[Release driver back to available pool]
```

### 2.4 Key Design Points
- **Geo-indexing**: Uber uses H3 (hexagonal hierarchical index) to bucket drivers/riders into cells for fast proximity search
- **Driver state**: Kept in-memory (Redis) for speed; periodically persisted
- **Surge pricing**: Computed per geo-cell using real-time supply/demand ratio (streaming via Kafka/Flink)
- **Consistency tradeoff**: Location data is AP (eventual consistency ok); trip/payment data is CP (strong consistency)
- **Scalability**: Location ingestion service must handle millions of GPS pings/sec — uses sharded, in-memory stores

---

## 3. WHATSAPP (Real-Time Messaging Platform)

### 3.1 Requirements

**Functional**
- 1:1 and group messaging
- Message delivery status (sent/delivered/read)
- Media sharing (images, video, docs)
- End-to-end encryption
- Online/last-seen presence

**Non-Functional**
- Extremely low latency (< 100ms for delivery)
- High availability, message durability (no message loss)
- Massive scale — billions of messages/day
- Support offline delivery (store & forward)

### 3.2 High-Level Design (HLD)

```mermaid
flowchart TD
    SenderApp[Sender App] --> Gateway[Connection Gateway - WebSocket/XMPP]
    Gateway --> SessionMgr[Session Manager - tracks active connections]
    Gateway --> MsgSvc[Message Service]
    MsgSvc --> Router{Is recipient online?}
    Router -- Yes --> PushDirect[Push message via recipient's active WebSocket connection]
    Router -- No --> Queue[(Offline Message Queue / Store)]

    MsgSvc --> MsgStore[(Message Store - sharded NoSQL, e.g., Cassandra)]
    MsgSvc --> Presence[Presence Service - Redis]
    MsgSvc --> Media[Media Service]
    Media --> Blob[(Blob Storage - S3-like)]

    Queue --> PushNotif[Push Notification Service - FCM/APNS]
    PushNotif --> RecipientApp[Recipient App - wakes up, fetches on reconnect]

    MsgSvc --> Kafka[(Kafka - delivery/read receipts)]
    Kafka --> SenderApp
```

### 3.3 Low-Level Design (LLD) — Message Send Flow

```mermaid
flowchart TD
    A[User A sends message to User B] --> B[Message encrypted client-side, E2E]
    B --> C[Sent to Connection Gateway over persistent WebSocket]
    C --> D[Message Service assigns message ID, timestamp]
    D --> E[Persist message to Message Store]
    E --> F{Is User B connected to any gateway server?}
    F -- Yes --> G[Look up B's server via Session Manager]
    G --> H[Forward message to that gateway, push to B in real time]
    H --> I[B's client sends Delivered ack]
    F -- No --> J[Store in Offline Queue for B]
    J --> K[Trigger push notification via FCM/APNS]
    K --> L[B comes online later, fetches pending messages]
    L --> I
    I --> M[Delivered receipt routed back to A]
    M --> N[When B opens chat, Read receipt sent back to A]
```

### 3.4 Key Design Points
- **Connection layer**: Persistent WebSocket/XMPP connections; each user pinned to one gateway server, tracked via Session Manager (Redis)
- **Message storage**: Sharded by user ID (Cassandra-like) for horizontal scale + durability
- **Delivery guarantee**: At-least-once delivery with client-side dedup using message IDs
- **E2E Encryption**: Signal Protocol — keys never leave devices; server only relays ciphertext
- **Group messages**: Fan-out on write — server duplicates message to each group member's queue
- **Presence**: Ephemeral, stored in fast in-memory store (Redis), not durable DB

---

## 4. NETFLIX (Video Streaming Platform)

### 4.1 Requirements

**Functional**
- Browse/search catalog, personalized recommendations
- Adaptive-bitrate video streaming
- Resume playback across devices
- Multiple simultaneous streams per account

**Non-Functional**
- High availability (global, multi-region)
- Low startup latency & buffering
- Massive read-heavy scale (catalog/recommendations)
- Handle huge bandwidth via CDN

### 4.2 High-Level Design (HLD)

```mermaid
flowchart TD
    Client[Client App: TV/Mobile/Web] --> APIGW[API Gateway / Zuul]
    APIGW --> UserSvc[User/Profile Service]
    APIGW --> CatalogSvc[Catalog Service]
    APIGW --> RecSvc[Recommendation Service]
    APIGW --> PlaybackSvc[Playback/Licensing Service]

    CatalogSvc --> CatalogDB[(Catalog DB)]
    RecSvc --> MLModels[ML Models - Spark/offline trained]
    RecSvc --> UserEvents[(User Activity Stream - Kafka)]

    PlaybackSvc --> CDN[Open Connect CDN - edge caches]
    Encoding[Encoding Pipeline] --> Storage[(S3 - Master Video Storage)]
    Storage --> CDN

    UserEvents --> Analytics[Analytics/Data Pipeline]
    Analytics --> MLModels

    Client --> CDN
```

### 4.3 Low-Level Design (LLD) — Video Playback Flow

```mermaid
flowchart TD
    A[User clicks Play on a title] --> B[Client requests playback manifest from Playback Service]
    B --> C[Check user's subscription/licensing rights]
    C --> D{Authorized?}
    D -- No --> E[Return error/upgrade prompt]
    D -- Yes --> F[Determine optimal CDN edge server nearest to user]
    F --> G[Return manifest: available resolutions/bitrates + CDN URLs]
    G --> H[Client player starts adaptive bitrate streaming - ABR]
    H --> I[Player monitors network bandwidth continuously]
    I --> J{Bandwidth drop/increase detected?}
    J -- Yes --> K[Switch bitrate up/down dynamically, fetch next chunk at new quality]
    J -- No --> L[Continue fetching chunks at current bitrate]
    K --> M[Periodically send playback position to server]
    L --> M
    M --> N[Resume-point saved for cross-device continuation]
```

### 4.4 Key Design Points
- **Content delivery**: Netflix Open Connect — custom CDN with appliances placed inside ISPs for edge caching
- **Encoding**: Each video pre-encoded into multiple resolutions/bitrates (H.264/AV1) and chunked (for adaptive streaming, e.g., DASH/HLS)
- **Recommendation engine**: Offline batch ML models (collaborative filtering, deep learning) refreshed periodically; served via low-latency lookup service
- **Microservices**: Netflix pioneered this pattern — hundreds of independently deployable services behind API Gateway (Zuul)
- **Resilience**: Circuit breakers (Hystrix) to prevent cascading failures across microservices
- **Data**: Cassandra for high-write viewing history; MySQL/EVCache for catalog and metadata

---

## Quick Comparison Table

| Aspect | Zomato | Uber | WhatsApp | Netflix |
|---|---|---|---|---|
| Core challenge | Order + delivery matching | Real-time geo-matching | Real-time message delivery at scale | Content delivery at scale |
| Consistency model | Strong (payments), eventual (search) | AP (location), CP (trips/payment) | At-least-once delivery | Eventually consistent catalog |
| Key data store | SQL + Elasticsearch | Geo-index (H3) + SQL | Sharded NoSQL (Cassandra-like) | Cassandra + S3 + CDN |
| Bottleneck | Search & dispatch latency | Location ingestion throughput | Connection/session management | Bandwidth & video encoding |
| Special tech | Redis GEO | H3 hexagonal indexing | Signal Protocol (E2E) | Open Connect CDN, ABR streaming |

---

*Note: These are simplified interview-style system design overviews. Real production systems at these companies involve far more nuance (sharding strategies, consensus protocols, multi-region failover, cost optimization, etc.) beyond what's covered here.*
