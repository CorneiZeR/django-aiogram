# Changelog

## 4.0.1 - unreleased

The package is untouched: this release moves the documentation and nothing else.

### Documentation

- **The documentation is a site now, not a wiki.** The same pages, in the same files, built by
  Material for MkDocs and published to GitHub Pages: navigation that groups the twenty-one
  pages the way the sidebar did, search that reads the prose, a dark theme and a version
  selector — none of which a GitHub wiki offers, and all of which a reference with thirteen
  settings tables and a 662-line upgrade guide has earned.

  `docs/wiki/` stays the source directory, deliberately: those file names are the page names
  the wiki published for three releases, so every inbound link maps one-to-one onto the new
  site, and the tests that read the pages by name — transport pages, check ids, event kinds,
  refusal catalogues, executed recipes, measured spans — keep working unchanged.

  **The 120 wiki links are relative Markdown links now.** The wiki's double-bracket form
  renders as literal brackets anywhere else, so it had to go; `[Delivery](Delivery.md)` renders
  in the repository *and* resolves into the site. Not through a plugin: python-markdown's `wikilinks`
  extension does not understand the piped form at all — measured, it renders the label
  `Event-log|Event log` — and 35 of the 120 were piped.

  **Case matters now, and did not before.** The wiki resolved `rate-limits` to `Rate-limits`;
  static files do not, so a link differing only in case used to work and would now 404.
  `tests/test_wiki.py` states the rule for every page on every CI leg, and the new `docs` job
  runs `mkdocs build --strict`, which refuses a missing target, a fragment no heading offers,
  and a page absent from the navigation — each proved by adding one and watching the build
  abort.

  `Home.md` is `index.md`, because that is what answers at the root of a version directory.
  `_Sidebar.md` is gone: its four groups are the `nav:` block, and the test that checked the
  sidebar listed every page now checks the navigation does.

  The site inherits the palette and the two IBM Plex faces from the repository's social card
  rather than inventing a second identity. The greens are not used at card strength for text —
  measured against the light ground, `#44b78b` is 2.16:1 — so each scheme carries the shade
  that passes: `#1f7a5c` on paper at 4.53:1, `#44b78b` on ink at 7.60:1.

  **The site is at <https://corneizer.github.io/django-aiogram/>**, versioned by `mike`: a push
  to `master` publishes the series `__version__` names and moves the `latest` alias onto it, so
  `/latest/` is what `master` says and a series freezes by itself the day the version moves
  past it. The 25 links in the README, the one in `AGENTS.md` and `project.urls.Documentation`
  point there — the last at the site root, so PyPI's sidebar follows the default-version
  redirect rather than freezing on the series that was current when a release shipped.

  Every page of the old wiki is a line pointing at its replacement. A wiki page cannot
  redirect — GitHub strips a meta refresh out of the Markdown it renders — so a reader gets a
  link to click, and the sidebar they see beside it says the same. `.github/workflows/wiki.yml`
  is deleted rather than narrowed to manual dispatch: the pages it would copy carry relative
  links, which a wiki does not resolve, so the one thing a dispatch could still do was break
  every link on it.

  **Case matters in the README too, now.** The check that every page is linked from the front
  page compared both sides case-folded, because the wiki resolved `rate-limits` to
  `Rate-limits`; on the site that is a 404, so the comparison is exact and the case that used
  to prove leniency now proves the opposite.

## 4.0.0 - 2026-08-31

**The package is `django-aiogram` now**, and Redis is one transport among several rather
than the only one. Import from `django_aiogram`, set environment variables with
`DJANGO_AIOGRAM_`, and read logs from the `django_aiogram` logger. The event log lives in
a new table, so the old one is left where it is — see **Upgrading** for moving the rows.

### Added

- **The names that said Redis are gone.** `send_redis` is **`enqueue`** and `asend_redis` is
  **`aenqueue`**: what they do is put a message on the queue, and which queue that is has been a
  setting since `BROKER`. `bot.send()` is untouched — it delivers directly inside the worker and
  queues everywhere else, and `enqueue` is what it calls when it queues.

  Removed rather than aliased, which is the 4.0 rule everywhere: an alias that outlives the
  release it was written for becomes a second name for the same thing for ever. `Upgrading.md`
  lists each old name against what to call instead.

  `bot.redis_conn` is gone too, and `get_redis` and `redis_conn` have left the package's
  exports. Those two are a **move, not a removal** — same objects, same laziness, still one
  connection — and `from django_aiogram.redis import get_redis, redis_conn` reaches them in the
  module that owns them. A client that can carry four transports should not answer for one of
  them, and a package that carries four should not export one's client from its front door.

  The settings keep their names: `REDIS_URL` and its neighbours say Redis because they *are*
  Redis's, and a project on the list transport writes exactly what it wrote before.

- **`BROKER`** — which transport carries messages, by dotted path, defaulting to the Redis
  list so a project changes its imports and nothing else. Nothing is inferred from what
  happens to be installed: a name whose driver is missing is a system check with the
  `pip install` line, not an `ImportError` on the first send.

  Each broker declares its **own** settings rather than adding to the package-wide table,
  because `REDIS_MESSAGES_KEY` means nothing to Kafka and a topic means nothing to a list.
  A transport with no sensible default for where a message goes marks that setting required
  and refuses at startup without it.

- **A Kafka transport**, `django_aiogram.broker.kafka.KafkaBroker`, on the **`[kafka]`** extra.
  The fourth, and the one whose model differs most: the other three settle a *message*, and
  Kafka settles a *position*.

  **`confluent-kafka`, not `aiokafka`**, decided by the consumer being a thread: a synchronous
  driver belongs in one, and an async-native driver would need an event loop inside it. On
  Apache Kafka 4 over loopback, both waiting for the broker, it is the faster one as well:

  ```text
                                    queued locally   waited for the ack
  confluent-kafka, synchronous           0.2 - 0.3us          166 - 295us
  aiokafka, handed to a loop thread        66 - 75us          351 - 492us
  ```

  Ranges rather than single numbers, and that is the point: a first run of this showed 479
  against 502 and "latency does not decide it" was written down as the finding. Repeating it on
  a warm broker said 1.3 to 2.2 times instead — the parity was a cold cluster. The decision
  stands and its stated reason changed; `scripts/measurements` is kept so the next reader can
  re-take them rather than trust either reading.

  The plan's other argument turned out to be false and is recorded so it is not reopened:
  `aiokafka` ships no `py3-none-any` wheel either, so both drivers are compiled and there is no
  portability difference.

  **A publish waits for the broker** — 166 to 295µs for one message, across eleven runs. On the
  footing every number in this release is measured on, a Redis list publish is 120–147µs and
  RabbitMQ's confirmed one 323–393, which puts this between them. Not divided against them: the
  three come from scripts run separately, so there is no single run to take a ratio from.
  `produce()` answers in 0.2µs because librdkafka's own thread does the I/O, and returning
  there would be weaker than the promise `RPUSH` already makes.

  Which is why the producer sets **`linger.ms` to 0**. The driver holds a batch open for 5ms by
  default, waiting for records to join it, and a send here is a batch of one that then waits for
  the broker's answer — so the default is paid in full on every `bot.send()`: measured, 6.4ms
  against 241µs, 26× for batching nothing. Batching still happens; what is switched off is
  waiting for it, and that is measured too rather than hoped for: one `publish` of a hundred
  payloads costs 0.44ms at 0 against 7.01ms on the default, which is 4.4µs a message — so the
  bulk path is faster as well, not traded away for the single one. Found by asking of this
  script the question worth asking of any of them: does it measure what the transport does?

  **Offsets are committed only where they are contiguous, per partition.** A consumer holding
  several sends cannot settle them in whatever order they finish: committing the second while
  the first is in flight would claim the first as done. So the broker commits the highest
  contiguous prefix for each partition and holds anything settled above a gap until the gap
  closes; partitions do not wait for each other.

  `release` rewinds that partition to the offset, so the released record **and** every later
  record in that partition is delivered again — Kafka has no per-message nack, and that is the
  honest consequence rather than a pretence.

  What a rewind is remembered by is bounded. A handle carries how many rewinds its partition had
  seen when it was taken, so settling can ask whether any rewind *since* then reached its offset
  — and that history is compacted the moment the partition has nothing in flight and nothing
  waiting to be committed, because at that point no handle the broker still expects can need an
  entry. Without it, a worker whose downstream is failing pays one integer per refusal for as
  long as it runs. A handle older than the compaction point is refused as rewound rather than as
  unknown, which keeps the log saying the thing an operator needs to hear.

  **A full local queue is waited out, not raised.** `produce` refuses with `BufferError` once
  librdkafka's own queue fills, which is what a broker that has stopped accepting looks like
  from inside the process after enough sends. That used to escape mid-batch: the records already
  accepted were in flight with nobody waiting for their callbacks, and the caller was told the
  whole batch had failed, so a retry sent the accepted prefix twice. `publish` now polls for
  room until its timeout, and if a record still cannot be queued it says how many never got in
  *and* how many were accepted before them — a batch that only partly went cannot be retried
  blindly, and Kafka has no way to un-send the half that went.

  A handle the rewind *reached* stops being settleable — one naming its offset or a higher one —
  and a send that finishes afterwards is reported and commits nothing, because accepting it
  could commit past messages the rewind put back. Handles below the rewind are untouched and
  still this worker's to settle, which is why the broker records where each rewind went rather
  than how many there have been: counting them refuses the lot, and the lowest of those then
  blocks the partition's commits for ever.

  `KAFKA_BOOTSTRAP` and `KAFKA_TOPIC` are required. Nothing here needs `WORKER_NAME`: a
  consumer that dies stops heartbeating, the group rebalances, and its partitions go to another
  member from the last committed offset.

- **A RabbitMQ transport**, `django_aiogram.broker.rabbitmq.RabbitMQBroker`, on the
  **`[rabbitmq]`** extra. The transport that needs least from this package: an unacknowledged
  message returns to the queue when the channel that held it drops, so there is no worker name
  to keep, no in-flight list to reclaim, and `reclaim()` answers `None` rather than zero.

  **`pika`, not `aio-pika`, and by measurement.** The obvious reading — async-native driver for
  the `await` half — is wrong, and the first measurement that said so was wrong too:
  `aio_pika.Connection.channel()` confirms publishes by default and `pika`'s does not, so
  comparing them directly put a confirmed publish next to fire-and-forget. Held constant, on
  RabbitMQ 4 over loopback, medians of 300 publishes, six runs, each row emptying the queue
  first and publishing exactly what this transport publishes — persistent and `mandatory`, with
  only the confirm varying:

  ```text
                                          unconfirmed     confirmed
  pika, on its own synchronous face          15 - 20us   323 - 393us
  aio-pika, reached by a sync caller       121 - 131us   456 - 511us
  pika, reached from a coroutine            67 - 85us   404 - 406us
  ```

  Each driver has exactly one face it must bridge, and **the two bridges do not cost the same**.
  Reaching a thread from a coroutine is 67–85µs; reaching a loop from a synchronous caller is
  121–131µs — about half, 0.53 to 0.64 times. An earlier version of this measured pika's bridge
  from the caller instead of from the loop, which added the hand-off the other row *is*, and the
  two then looked like the same number; the conclusion drawn from that — "the hand-off is the
  price, not the library" — was true of the arithmetic and not of the drivers.

  So `pika` wins twice. It is free on the face this package's traffic uses: `bot.send()` is
  called from views, tasks and management commands, at 15–20µs unconfirmed with nothing to
  bridge. And it is the cheaper of the two on the rare face, where `asend()` reaches it from a
  coroutine. `aio-pika` also cannot serve a synchronous caller simply: its connections are
  loop-affine, so `async_to_sync` over one built elsewhere raises `attached to a different
  loop`, exactly as `redis.asyncio` does.

  **Publishes are confirmed, mandatory and persistent**, which costs 323–393µs against 15–20µs
  for the same publish with only the confirm taken off. That buys the promise the package
  already makes: `RPUSH` answers with the new length, so a Redis publish is acknowledged before
  `send()` returns and a failure raises. Unconfirmed AMQP publishing is weaker than that — a
  broker that dies before persisting the message loses it in silence — so this is the most
  expensive publish of the four, above Kafka's 166–295µs and a Redis list publish's 120–147 on the
  same machine and virtualisation. Those figures are not divided against each other: they come
  from different scripts run at different times, so there is no pair from one run to take a ratio
  from, and an ordering is what they support. That ordering is the guarantee rather than the
  driver, and most of it is the disk: dropping persistence alone takes the same publish to
  135–173µs.

  **The multiple is not portable, and earlier drafts of this entry quoted one that was not
  measured.** All four numbers here come from brokers in containers on one laptop, where the
  virtualised loopback is most of a Redis publish; 3.1.0 measured a native list publish at
  14–19µs, and against *that* baseline the same AMQP publish is twenty times rather than two and
  a half. What survives a change of footing is the ordering — Streams ≤ list < Kafka < RabbitMQ
  — so `scripts/measurements/redis_baseline.py` is shipped alongside the other two to make the
  divisor visible rather than assumed.

  `RABBITMQ_URL` and `RABBITMQ_QUEUE` are both required. `RABBITMQ_PREFETCH` defaults to
  unlimited on purpose: `MAX_IN_FLIGHT` already bounds unacknowledged sends, and a prefetch
  below it would stall the consumer.

- **A Redis Streams transport**, `django_aiogram.broker.redis_streams.RedisStreamsBroker`.
  The same server and the same `[redis]` extra as the list — a different data structure, not a
  different dependency — and the first transport here that names a message by id rather than
  by value, which is the model RabbitMQ and Kafka use.

  It needs **Redis 7.0**, for this transport alone; the package floor stays 6.2 for the list.
  `XINFO GROUPS` grew the `lag` field in 7.0 and it is the only exact answer to how many
  messages are waiting, so the broker probes for the field on first use and refuses by name
  without it rather than reporting a number that would drive `HEALTHCHECK_MAX_QUEUE` wrongly.
  Probed rather than read off a version string, for the same reason the list probes `LMOVE`.

  `REDIS_STREAM_KEY` is **required** and has no default on purpose: a default would sit a
  suffix away from `REDIS_MESSAGES_KEY`, and `XADD` against a key holding a list answers
  `WRONGTYPE` on the first send instead of at startup. `REDIS_STREAM_GROUP` defaults.

  Two things it does that the list does not need to:

  - **`WORKER_NAME` buys nothing here.** The pending list belongs to the group, so any
    consumer can recover a dead one's work — measured, `XAUTOCLAIM` under a name that never
    existed before claims every entry the dead consumer held. A container returning with a
    fresh Docker hostname strands nothing, which is the one thing `I001` warns about for the
    list. `needs_identity` is `False` and the checks read it.
  - **`release()` is not a no-op.** Leaving an entry pending is not enough: `reclaim` only
    takes entries idle past the liveness TTL, so a message refused a second after it was
    taken would sit unsent for that long. `XCLAIM … IDLE` sets the counter to exactly that
    threshold, and the boundary is inclusive — measured on a real server and on fakeredis.

  **Never trim this stream by length.** `MAXLEN` and `XDEL` remove exactly the entries an
  unfinished send leaves unacknowledged: trim past a pending entry and `XPENDING` still
  reports it while `XAUTOCLAIM` hands the id back in its *deleted* list, so the message is
  gone and nothing can replay it. `XDEL` additionally costs Redis the ability to answer `lag`
  at all, and a depth read then refuses instead of guessing — temporarily: measured, the count
  returns once the group has read to the end of the stream, so it is missing exactly while
  there is a backlog. The
  broker's own `trim()` stops at the oldest unacknowledged entry.

- **A page per transport in the wiki** — **Redis list**, **Redis Streams**, **RabbitMQ** and
  **Kafka** — each stating what it guarantees, which services must be installed and running, what a
  publish costs with the figure measured rather than remembered, and where its own machinery
  shows through. **Delivery** is about the contract now and points at them for the rest, with a
  table of the three differences that change an operator's job: crash safety, whether a worker
  needs a name that survives its container, and whether recovery is a command, a clock, or the
  broker's own doing. `Deployment` carries a compose recipe for each and `Troubleshooting` the
  duplicate each one produces.

- **Every event log row written since the column arrived carries a short id, and the admin searches
  by it.** Twelve characters of the
  correlation id in Crockford's base32 — the alphabet without `I`, `L`, `O` and `U` — stored in a
  new indexed `short_id` column and shown in the thread column of the changelist.

  The column used to show the correlation id's first eight characters, which is a clock and not an
  identifier: a UUIDv7 opens with a 48-bit millisecond, so a hundred messages sent in one instant
  showed the same eight characters — measured — and typing them back found nothing, because Django
  refuses a partial UUID before the query is built. The one thing the page invited a reader to do
  was the one thing it could not answer.

  Reading a code back ignores case, spaces and hyphens, and folds `I`, `L` and `O` onto the
  characters they are heard as, so one read out over a call still lands. `U` is dropped from the
  alphabet without being folded: Crockford leaves it out to make an accidental obscenity less
  likely, and aliasing it onto `V` would turn a mistyped code into a different valid one. It is stored rather than
  computed because base32 of a truncated id has no inverse a `WHERE` clause can use — a computed
  code could only be searched by scanning the table. Sixty random bits make a collision unlikely
  rather than impossible, so the column is not unique: the search returns every row a code matches,
  and `correlation_id` stays the identifier. Measured at +57 bytes a row with its index.

  `migrate` adds the column empty and **`manage.py tgbot_backfill_short_ids`** fills what is already
  there, in bounded chunks with the same `--chunk`, `--sleep`, `--max-chunks`, `--database` and
  `--dry-run` as the other jobs. A row with no code is a row still to do, so there is no watermark
  to keep: a stopped run resumes, a finished table reports and writes nothing. Not a data migration,
  for the reason the prune is not one — `migrate` runs inside a deploy, where a table sized by
  traffic cannot be paced or stopped. Until it is run, rows from before it read `(not backfilled)`
  and still link to the rest of their thread.

  `tgbot_move_events` now names the columns the two tables **share**, asked of the database rather
  than taken from the model, and writes the model's default for the ones only this release has. The
  model described both tables while they agreed, and this is the release that ends that: selecting
  `short_id` from the 3.x table is an error from the backend, and leaving it out of the insert is a
  not-null violation, since `migrate` adds the column with a default and then drops it. Rows moved
  from 3.x arrive with no code, which is what the backfill above looks for — run it after the move.

- **`manage.py tgbot_move_events` copies the 3.x event log into this release's table**, and `I003`
  says it is there to be copied. The rename left `django_redis_aiogram_event` where it was, and the
  upgrade page offered one `INSERT ... SELECT` — honest, and the wrong shape on a table sized by
  traffic: one statement, no pacing, no way to resume, and a sequence left behind.

  It walks primary-key ranges like `tgbot_prune_events` does, with the same `--chunk`, `--sleep`,
  `--max-chunks`, `--database` and `--dry-run`. Both tables share the primary key, so an id already
  in the new table is either a row the command copied or a row this release wrote — either way it is
  not inserted again, and each chunk copies only the ids that are not there yet. A killed run
  resumes at the first id the destination does not have, a second run copies nothing, and running it
  twice is not a way to duplicate history.

  What it could not copy is counted across the whole old table and named on every run, including the
  one that copies nothing. Counting it from the ranges the walk visited would miss exactly the rows
  most likely to be lost: a destination already holding low ids puts the resume point above them, no
  chunk ever looks, the move reports a clean total, and every later run says every id is present —
  which is the state in which somebody drops the old table. A row is told from a copy of itself by
  comparing every column — two rows can share an id, a timestamp and a kind and still differ in the
  payload or the chat — and the count is taken *after* the copy, so a row that lands while a chunk
  is running is counted too.

  Starting above the destination's *highest* id would be cheaper and silently wrong: the bot writes
  to the new table from the first message after `migrate`, so a destination that has been
  written to at all would skip every old row beneath it and report a completed move. An id a row of this release already
  holds is left where it is and counted in the report, because nothing can put two rows under one
  primary key and renumbering somebody's history is not a decision this command gets to make.

  Columns are named one by one rather than `SELECT *` — the two tables agree today and would keep
  agreeing right up to the release that changes either of them, where a mismatched column count is
  rejected and a matching count in a different order is accepted with every value one column to the
  side. Naming them fails on the first and makes the second impossible.

  Then it moves the sequence past the ids it inserted, which is the step a hand-written copy leaves
  out: explicit ids do not advance a PostgreSQL sequence, so the copy succeeds and the *bot's* next
  write fails on a duplicate primary key, at whatever hour the first message after the migration
  arrives. Django's own `sequence_reset_sql` does it, which is what `loaddata` uses for the same
  reason and speaks each backend's version.

  The sequence is reset on the way out of a run that finds nothing to copy, too. A run killed
  between its last chunk committing and that reset leaves the ids copied and the sequence behind
  them — and every rerun then takes the "nothing is left" path, so without this the deployment stays
  one duplicate-key error away from its next write with a command that reports success each time.

  A chunk that loses a race is retried. `NOT EXISTS` chooses the ids to insert and does not hold
  them, so a row this release writes in that gap ends the insert as a unique violation — likeliest
  of all before the command has ever run, when the destination's sequence is still where `migrate`
  left it and the bot is drawing the very ids the old table used. Each retry excludes what landed,
  so it converges; a chunk that keeps colliding names its range and says to move the rest while
  nothing is writing, rather than ending the run with a traceback about a primary key.

  A database that cannot be read stops the command and leaves the check quiet, which are opposite
  answers to one question on purpose: a rule that raises takes `manage.py check` down, while "there
  is no old table" and "I could not look" read the same to a cron job and only one of them means the
  history has been moved.

  Not a data migration, deliberately: `migrate` runs inside a deploy, and a copy that holds one open
  for as long as the table is large cannot be paced, resumed or stopped. The check makes the command
  discoverable; the operator picks the night. Nothing is dropped —
  `DROP TABLE django_redis_aiogram_event` stays theirs to run.

  The suite grew a leg for this: `tests/postgres_settings.py` runs the database-backed cases against
  a real PostgreSQL, because `sqlite_sequence` follows an explicit id on its own and a sequence does
  not. Written against SQLite alone, the case that matters here passes whether the command resets
  the sequence or not — measured, by deleting the reset and watching it stay green. Every case in
  `tests/db` runs on both backends, and where one cannot express the condition a fixture supplies
  it there and stays out of the way on the other: a connection that can really die, a close that
  really closes, and a planner asked with `enable_seqscan` off so its answer is about the index
  rather than about the size of a fixture.

### Fixed

- **The container healthcheck could not start without redis-py, so two transports had no
  probe at all.** `healthcheck.py` named `redis` at import time, which was right when the
  driver was a hard dependency and wrong the moment it became an extra: on a `[kafka]`-only
  or `[rabbitmq]`-only install, `python -m django_aiogram.healthcheck` answered
  `ModuleNotFoundError: No module named 'redis'`. **Deployment** documents that command as
  the container healthcheck, so the bot container was unhealthy for ever and the one thing
  that would have said why was the thing that crashed.

  Measured after, on a Kafka-only and a RabbitMQ-only venv built from the wheel, neither
  carrying redis-py: `healthy: consumer not observable from outside, 0 queued`, exit 0.

  The split the fix leans on was already in the code — the verdict comes from
  `broker.liveness()` and `broker.depth()`, both transport-neutral, and only `--guarantee`
  and `--stranded` are Redis-shaped. Those two build a client of their own, and where there
  is no driver each says so in its own way instead of refusing — `--guarantee` reads
  `unknown` in the line, and `--stranded` warns that the sweep did not finish, both logging
  the reason as `tg_reason`. On the three transports that key no in-flight work on a worker
  name, the sweep is not attempted at all. Neither could ever change the verdict. **Deployment** now has a table of what the probe
  can see on each of the four, because "stranded in-flight lists" means nothing on Kafka.

  Three more things came out of it, all in the same class:

  - **A `BROKER` whose driver is missing is now reported rather than raised.** Both forms of
    the probe print `RedisListBroker needs the 'redis' package, which is not installed.
    Install it with: pip install "django-aiogram[redis]"` and exit non-zero — an image built
    for one transport and pointed at another used to get a traceback out of a probe whose
    whole output is a diagnosis.
  - **`--stranded` says when it could not look.** The sweep returned "nothing found, and I
    finished" whenever its client could not be built — so a report on an install with no
    driver read as a clean zero about a question nothing had asked. It now warns
    `the scan for stranded in-flight lists did not finish: …` with the reason, for a missing
    client, a key it could not decode, or the twenty-`SCAN`-round bound alike. Where the
    transport has no worker-keyed in-flight work at all, the sweep is still not attempted and
    still says nothing: a warning twice a minute about a question Kafka does not have is
    worse than silence.
  - **One refusal changed wording:** `redis is unreachable: …` is **`the broker is
    unreachable: …`**. Up to 3.1 the probe pinged Redis before every check and could name it;
    it opens no client of its own now, so the transport is asked and the first thing it cannot
    do is what gets reported. A refused connection keeps that line rather than sliding into
    `could not read the consumer liveness`, which is what a broker that *was* reached and then
    failed the read gets — a failover mid-probe, a replica that cannot serve the key, a
    refusal, or any other error out of the call. **Upgrading** and **Troubleshooting** carry the change for anyone who greps it.

  A test hides `redis` from a fresh interpreter's `sys.modules` and asserts the probe still
  reports, because no venv in CI can show this: the dev environment installs every extra and
  the `[redis]`-only leg installs that one.

- **The suite's Redis fixture never put the real accessor back.** `tests/conftest.py` patches
  `get_redis` at five paths, and `monkeypatch.setattr` imports a module named by string — so a
  broker module first imported *there* bound the fake the first path had already installed, and
  monkeypatch recorded that fake as the original to restore. Measured: after one test that used
  `redis_server`, `broker.redis_list.broker.get_redis` was still a fixture lambda, so three
  healthcheck cases with an unusable `REDIS_URL` read a working fake and passed only when run
  alone. The targets are imported once at conftest import time now, before any patch.

- **The package described itself as Redis-only, on PyPI.** `description` said "send Telegram
  messages from Django **through Redis**" and `keywords` had no `rabbitmq`, no `kafka`, no
  `redis-streams` — the first sentence anybody reads on the project page, and the words people
  search by, both written before `BROKER` existed. Three pages said the same in passing and now
  name the broker instead: the assistant page's summary of `bot.send`, and two sentences on the
  **Webhook** page. The README's own diagram keeps the Redis list, which it labels as the default
  it shows.

- **Two comments and one docstring pointed at things that had moved.** `broker/registry.py` and
  `consumer/delivery.py` sent a reader to `producer.client` for the `ValueError` catch they share
  — it moved to `producer/from_settings.py` when the client was split — and `eventlog/signals.py`
  documented `EventRecorder._publish`, a method the recorder's split deleted, plus `Event` at the
  home it left.

  **A test resolves them now**, since prose is the one part of the package no gate reads:
  `tests/test_docstring_references.py` reads every `:mod:`, `:meth:`, `:attr:` and friend in the
  package — 148 of them — and resolves the 37 that name something inside it against the syntax
  tree of the module named. It found both, and it fails on either coming back. References into other
  packages are somebody else's to keep; a plain backticked mention is outside it, and the two
  comments above were fixed by hand for that reason.

- **`I003` introduced itself as a setting that does not exist.** Django prints the check id and
  then the message, so a keyless rule that inherits the settings name for a subject renders as
  `TELEGRAM_BOT django_redis_aiogram_event is still on the 'default' database` — which reads as a
  typo and sends a reader looking for `TELEGRAM_BOT['django_redis_aiogram_event']`. The rule is
  about a table, and says so: `Problem.label` already existed for exactly this, and `E048` was
  already using it for `DATABASE_ROUTERS`.

- **`migrate` no longer fails on every upgrade from 3.x.** The new table declared the four index
  names the old one already has — `drai_event_correlation`, `drai_event_recent`,
  `drai_event_kind_id`, `drai_event_chat` — and index names are unique per *schema*, not per
  table. So a project that followed **Upgrading**, which says to leave `django_redis_aiogram_event`
  where it is, met `relation "drai_event_correlation" already exists` at `0001_initial` and could
  go no further.

  They are `dja_event_*` now, which is also the honest prefix: `drai_` was
  *django-**r**edis-**ai**ogram*. **On PostgreSQL and SQLite** index names are unique per schema
  rather than per table — measured: PostgreSQL 17 refuses with `ProgrammingError`, SQLite with
  `index drai_event_chat already exists`. MySQL scopes them per table and never saw this. Found
  by running the documented upgrade on a real project, which is the only place it shows: every
  suite builds a database that has this app and nothing else in it.

  An installation of `4.0.0.dev0` that already migrated has the indexes under the old names, and
  **nothing reads an index by name at runtime**. Nothing will report it either: `makemigrations`
  compares the model with the migration files and never looks at the database — measured, it says
  `No changes detected` on exactly that database — so the names stay as they are until somebody
  renames them. To do that without touching a row:

  ```sql
  ALTER INDEX drai_event_correlation RENAME TO dja_event_correlation;  -- PostgreSQL
  ALTER INDEX drai_event_recent      RENAME TO dja_event_recent;
  ALTER INDEX drai_event_kind_id     RENAME TO dja_event_kind_id;
  ALTER INDEX drai_event_chat        RENAME TO dja_event_chat;
  ```

  MySQL spells the same rename `ALTER TABLE django_aiogram_event RENAME INDEX <old> TO <new>`,
  once for each of the four; SQLite has no rename at all, so there each is dropped and created
  again.

  One narrower state, measured rather than assumed: a `4.0.0.dev0` whose `migrate` stopped
  *between* `0001` and `0002` finishes on PostgreSQL without complaint — Django's PostgreSQL
  editor drops indexes with `IF EXISTS` — but leaves `drai_event_kind_recent` behind, an index
  nothing will ever drop and every insert pays for. `DROP INDEX drai_event_kind_recent;` is the
  whole cleanup. A fresh install is unaffected, and no released version ever created these
  names.

- **The two readers whose contract is to *report* survive a setting `int()` cannot read.**
  `int(float('inf'))` raises `OverflowError`, which neither guard caught: the container health check
  ended in a traceback instead of the `is not a number` it has ready — a restart loop with nothing
  to read — and the payload cap cost a logged traceback and an `undescribable` row on every event,
  where `E034` already reports the setting. Both catch it now; the readers that raise on an
  unreadable setting still do, because the checks refuse that configuration at startup.

- **A dotted path with an empty module part is refused by this package rather than by Django.**
  `'.Storage'` — what a copied path, an autocomplete or a half-written relative import produces —
  reaches `import_module('')`, which raises `ValueError` and not the `ImportError` the three
  resolvers caught. So `BROKER`, `DELIVERY` and `FSM_STORAGE` each handed back Django's
  `Empty module name` where they had a sentence ready naming the setting. Measured on Django 6.1;
  the two rules in `config/checks/` had the same gap and were fixed with the split.

- **A setting written as the enum member this package publishes is read as one.** `API.md` tells a
  project to write `'MODE': UpdateMode.POLLING`, and that raised `ImproperlyConfigured` at startup
  naming `'updatemode.polling'` — a value nobody typed. These enums mix in `str`, so a member
  compares equal to its own value, which is why `SERIALIZER` and `FSM_STORAGE` happened to work;
  but since 3.11 `str()` on a member gives its *name*, so every reader that normalised the text
  matched nothing.

  `EVENT_LOG_PAYLOAD` was the quiet one: `PayloadDetail.FULL` read as unreadable and fell back to
  summaries, so a project asking for full payloads got less than it configured with nothing said.

  Both go through `config.enums.as_member` now, with the checks' own copy of the idiom replaced by
  it, so the two spellings are one setting everywhere. And the documented block is executed by the
  suite: every `TELEGRAM_BOT = {...}` in the pages that names one of these enums is resolved and
  driven through the readers, so a page that documents a spelling the package cannot read fails
  here rather than in somebody's deployment.


- **The cap on a blocking take comes from the transport that is being read, and `W004` names it.**
  The consumer weighs `BLPOP_TIMEOUT` against `HEARTBEAT_INTERVAL` and a whole second inside the
  floored call deadline — and that deadline was `REDIS_TIMEOUT` whichever transport was configured. So a setting
  three of the four never read shortened their reads: measured on Kafka with `KAFKA_TIMEOUT = 30`,
  `REDIS_TIMEOUT: 2` capped the poll at **one second**, and `W004` told the operator to raise
  `REDIS_TIMEOUT` — a setting their deployment does not have. Now the same configuration caps at 10
  seconds, bound by `HEARTBEAT_INTERVAL`, and `REDIS_TIMEOUT` moves nothing.

  The other direction mattered too: a transport whose own timeout was *lower* than the Redis one
  got a cap looser than it allows, so a read could outlast `Broker.call_ceiling` — the number
  `start_tgbot` derives its join from, which is 3.1.0's B3 through the last door it had.

  `Broker.CALL_TIMEOUT_OPTION` names each transport's deadline setting and `Broker.call_timeout()`
  reads it, both on the class, because `W004` has to answer without building a broker — building
  one imports its driver, and a missing driver is `E047`'s report to make rather than this rule's.
  The conformance suite holds the name to the number in both directions, so the setting `W004`
  quotes cannot drift from the one the cap uses. `blpop_ceiling()` is `take_ceiling()` with the
  deadline passed in, and `PopCeiling` is `TakeCeiling`: three of the four transports issue no pop.

  The deadline is a **float** throughout, because `KAFKA_TIMEOUT` accepts fractions: read as an
  integer, `0.5` raised and left `W004` silent on a deployment whose poll is capped at a second,
  and `2.5` reported a ceiling one second away from the one the consumer applies. The cap floors
  it, so `2.6` allows one whole second inside the deadline rather than two.

  `call_timeout()` is also the only reader, and refuses what cannot be a deadline: not a number,
  or not above zero, or not finite. `RabbitMQBroker` read `RABBITMQ_TIMEOUT` a second time with an
  `or 10` on it, which fires on exactly the values a project writes and `or` treats as unset — so a
  configured `0` reached pika as ten while `W004` and the consumer's cap were computed from zero.
  Kafka's range check narrows the same reader instead of replacing it, which is how a non-numeric
  `KAFKA_TIMEOUT` stopped raising a bare `ValueError` naming nothing.

  On RabbitMQ that number now wins over a `blocked_connection_timeout` written into `RABBITMQ_URL`,
  where the URL used to win — the one pika parameter the package takes off a project, and for
  arithmetic rather than taste: the consumer's take is capped by `RABBITMQ_TIMEOUT` and
  `start_tgbot` derives its join from it, so a URL overriding it left both describing a deadline no
  call carried. Measured, a URL saying 60 against a setting of 2 left the join at 3 seconds while a
  blocked publish could sit for a minute. Every other parameter in the URL is still the project's,
  `socket_timeout` included — measured, that one does not bound a take: an idle 4-second `consume`
  returns empty under a `socket_timeout` of 1.

  The ceiling weighs its two bounds as pairs rather than as a mapping keyed by option name. A
  broker may name `HEARTBEAT_INTERVAL` as its own deadline option — `option` refuses only a
  *differing* default — and with an override returning a number other than the setting it names,
  the deadline entry replaced the heartbeat entry and the larger of the two won: a heartbeat of 2
  against a deadline of 100 capped the read at 99, so the consumer waits its own heartbeat out and
  is reaped while healthy. `bound_by` deduplicates, because the two names can be one name.

  The driver is verified **last**, after everything this rule can judge without one. Resolving the
  broker with the driver checked first meant the deadline findings were reachable only where the
  extra happened to be installed — and never at all in a process with `ENABLED` off, which returns
  early on a missing driver by design. That is where a settings typo is most likely to sit
  unnoticed, and it was also why two cases passed on a machine with every extra installed and
  failed on the unit legs, which install one.

  Every page that wrote the cap out was writing `<deadline> - 1`, which is false for a fractional
  one — and a test now holds prose to the helper by refusing a statement of the cap without its
  floor, deliberately grep-shaped: what went wrong was the formula being written a fourth time, and
  a test that computed the cap would have agreed with all four.

  `E047` grew two findings to match, neither gated on `ENABLED`: a broker declaring no
  `CALL_TIMEOUT_OPTION`, or naming an option it does not declare — the seam needs that name, and
  without this the gap surfaced as a `KeyError` out of whichever rule asked first, taking every
  other finding with it — and a deadline the transport refuses, which until now passed every rule
  and failed at the first send. `REDIS_TIMEOUT` had `E030` for this because it sits in the
  package-wide table; the other three sat with their transports where no rule reached them. It
  stands aside where the option has a rule of its own that is *already reporting* the value, asking
  the registry rather than matching a name — a broker somebody wrote can name `REDIS_TIMEOUT` and
  need more of it than `E030`'s minimum of 2, and suppressing on the name alone left that value
  unreported by everything until the transport first read it.

- **The Kafka producer states its acknowledgement level instead of inheriting one.** `publish`
  waits for the broker rather than returning once librdkafka has queued the record, and the
  guarantee that rests on `acks` was whatever the driver defaulted to — a decision nobody in this
  package had made, and one release of the driver away from being something else. It is `acks=all`
  now, with `enable.idempotence` on.

  Measured on a single-broker cluster: `all` costs nothing against the inherited default, 442 µs
  median against 452, because the inherited default *is* `all`; `acks=1` would buy 60 µs and a
  window where an acknowledged record lives on one log. Idempotence leaves the steady-state
  publish unchanged at 431 µs and costs one PID acquisition per process — the first publish paid
  1.0 second once on a cold coordinator, in a container that runs for weeks. What it buys is no
  duplicate from librdkafka's own retries, which are unbounded by default: a retried produce after
  a partial success writes the record twice, and a duplicate here is a second Telegram message
  that no consumer-side key can deduplicate.

  **Kafka** explains what the acknowledgement does and does not mean, including the part a single
  broker cannot give you: one in-sync replica means `all` and `1` acknowledge the same write, and
  replication is what makes the difference real.

- **The bot process resets a dead database connection, which it never did.** Django closes a
  broken or obsolete connection in `close_old_connections()`, wired to the `request_started` and
  `request_finished` signals only — and a bot worker has no requests. So a connection that died
  once stayed dead in that process for ever: measured in production on 2026-08-27, Postgres
  restarted at 23:43, the first update after it arrived at 03:22, and every handler raised
  `InterfaceError: connection already closed` for the next ten hours until the container was
  restarted. `CONN_MAX_AGE` was never the fix; at `0`, the default, the connection is still only
  closed inside that same request hook.

  **Outbound delivery was unaffected, and that is what made it invisible.** The consumer kept
  sending and `python -m django_aiogram.healthcheck` kept passing, because neither touches the
  database — so `docker ps` and the probe both said the bot was fine while every deeplink and
  every inline button silently did nothing. aiogram logs the exception and leaves the update
  unhandled, so the person pressing the button gets no answer at all. **Deployment** now says
  which class of failure the probe cannot see.

  Every update is bracketed with the reset now, by an unconditional outer middleware registered
  where the dispatcher is built — so polling and webhook both get it, and it is the outermost
  frame rather than one behind the event log's. Before *and* after, because each side does a
  different job: the leading close recovers from a connection that died while the process was
  idle, and the trailing one returns the connection at `CONN_MAX_AGE=0` instead of parking an open
  socket per worker between updates.

  `thread_sensitive=True` on the `sync_to_async` is load-bearing rather than a default copied
  over, and the suite pins it: handlers reach the ORM on asgiref's thread-sensitive executor, a
  Django connection is thread-local, and a reset on the loop thread would close a connection
  nobody used — the fix would look applied and change nothing. Dropping the flag makes the
  recovery case fail with the outage's own error and the thread case report two different threads.

- **Naming a worker in `inflight_depth` asks the transport, not Redis.** The unnamed call went
  through the broker seam; naming somebody else reached a Redis client directly, whatever `BROKER`
  said. On a RabbitMQ or Kafka deployment that either raised about a missing `REDIS_URL` or —
  worse — answered `0` from an unrelated Redis the project happened to run for caching, which is
  the one answer that stops a monitor looking.

  The seam takes the name now, and the four transports answer as they can. The Redis list reads
  the key it already keeps per worker; Redis Streams narrows to one consumer's share of the
  group's pending list, from the per-consumer breakdown that rides along in the `XPENDING`
  summary it already fetches, so a name costs no extra round trip. RabbitMQ and Kafka raise the
  new **`WorkerDepthUnavailableError`**, carrying the transport and the worker asked about:
  unsettled work there belongs to a channel or a group member rather than to a name, and what a
  dead worker held is already back in `queue_depth()` — the broker requeues, the group replays.

  With that, `producer/client.py` imports nothing from the Redis module for the depth reads at
  all. A test suite that patched `django_aiogram.producer.client.get_redis` should patch the
  broker's client instead — `django_aiogram.broker.redis_list.broker.get_redis` — which is what
  **Testing** already tells the reader to do for sends.

- **A RabbitMQ connection is no longer held after the thread that owned it ends.** The client
  keeps a registry of every connection this process opened, because pika allows another thread
  exactly one operation on a `BlockingConnection` — `add_callback_threadsafe` — so a shutdown can
  only *ask* a foreign connection to close, and asking needs a reference. That reference was
  strong and pruned only by a close that went through the client, so a worker thread that exited
  without closing left its connection, and its socket, for the life of the process. The registry
  is a `WeakSet` now: measured, a connection built on a pool worker is collected as soon as that
  worker is gone.

- **A refusal keeps what a caller would branch on.** `ProduceRefusedError` kept the topic and
  what librdkafka said; `QueueRefusedError` kept the queue and formatted the reason into its
  message, so the same retry decision on RabbitMQ meant matching on English. Both carry
  `reason` now, and the sweep for the rest of the class kept five more values, on five more
  refusals: `function` on `UnknownApiMethodError`, `timeout` on `LoopThreadNotStartedError`,
  `version` on `UnknownEnvelopeVersionError`, and `kind` and `name` on the two serializer
  refusals that read a queued payload.

  Six are deliberately *not* kept, and the reason is the same in each case: the value is one the
  holder of the exception can already answer for — its own settings, its own object's type, or a
  count of the installed aiogram's method table. `API.md` publishes what each documented refusal
  carries, and the suite holds the page to it in both directions.

- **The shutdown join is bounded by the transport being joined, not by `REDIS_TIMEOUT`.** That
  setting bounded the consumer thread on all four transports, three of which never read it — so
  the join could give up while the thread was still inside a legitimate call, and a worker that
  outlives its join goes on to acknowledge a message `close()` has already refused, which is
  3.1.0's B3 arriving through a different door. Measured at `KAFKA_TIMEOUT = 45`: the join now
  waits 46 seconds where it used to wait 11.

  `Broker.call_ceiling` is the seam's answer to how long one call into a transport may take,
  abstract so a new one cannot forget it. The published grace arithmetic in **Deployment** follows
  it; on the default transport with default settings the number is unchanged, which is exactly why
  this was invisible — every shipped transport defaults its own timeout to 10, so the wrong
  arithmetic produced the right answer everywhere anybody looked.

- **The transport is closed at shutdown, which it never was.** `Broker.close()` documented
  itself as "called once, at shutdown" and the only path to it was the `setting_changed`
  receiver — a signal that fires in a test suite and never in a deployment. `bot.close()` tore
  down the loop, the aiogram session and the FSM storage and left the queue's connection alone,
  so neither long-running command reached it, and a web process that only queues had no
  shutdown path at all.

  Worst on Kafka, in two ways. The producer was never flushed, so the warning it raises about
  messages accepted locally and never delivered could only fire in tests. And the consumer never
  left its group: a member that disappears without saying so holds its partitions until the
  session times out, so a restarted bot container delivered **nothing** for that long, with a
  full queue and a worker that looked healthy.

  Closed by two paths now, because neither covers the other. `bot.close()` releases it after the
  consumer thread is joined, which is deterministic and can still report what it failed to flush.
  An `atexit` hook, armed the first time a broker is built, covers every process that closes
  nothing — the same shape the event log's writer has used all along.

  Neither path can promise which thread it runs on. `atexit` runs during interpreter shutdown, on
  the main thread; `bot.close()` runs wherever it is called from, and nothing restricts that. So
  neither is guaranteed to be the thread that opened a given connection, which is why each
  transport's `close()` already restricts what it touches from a foreign one: pika asks the owning
  thread through `add_callback_threadsafe`, and librdkafka flushes the process producer and closes
  only the calling thread's consumer.

### Changed

- **`manage.py check` said nothing about the one extra a Kafka or RabbitMQ project still
  needs.** `FSM_STORAGE` defaults to `'redis'` and that store is aiogram's, which imports
  redis-py — so a project that installed its own transport's extra, left the setting alone and
  set a `REDIS_URL` passed every check and then died building the dispatcher. Measured on a
  `[kafka]`-only install: `System check identified no issues`, followed by
  `ModuleNotFoundError: No module named 'redis'` out of `start_tgbot`. A missing driver
  reaching a project as a traceback is the shape `E047` exists to prevent for the *broker*,
  and this was the same shape arriving through the storage door, where no rule was watching.

  The install lines follow it: the README and **Installation** show
  `pip install 'django-aiogram[kafka,redis]'` and the RabbitMQ equivalent, because one extra
  per transport is true of the *queue* and the FSM store is a second question. `pyproject.toml`
  says the same beside the extras it declares.

  `E019` judges the driver behind `'redis'` now, with both ways out in the hint — install the
  extra, or set `'memory'` if the deployment keeps no chat state — and `find_spec` rather than
  an import, because this rule runs on every `manage.py` invocation. **Installation** says it
  too, next to the install lines, since the table above it lists one extra per transport and
  that is what made the gap easy to walk into.

- **The release build's last gate could not run.** `publish.yml` hands the built artifacts to
  `scripts/smoke_install.sh dist` so that the wheel it uploads is the wheel that gets
  installed — and that branch of the script kept the path it was given, relative, then moved
  into a throwaway project where `dist/...` no longer resolved. So the step failed on
  `Requirement 'dist/django_aiogram-4.0.0-py3-none-any.whl[redis]' looks like a filename, but
  the file does not exist`, which would have stopped the publish after the tag was cut.

  Nothing had run it: CI called the script with no argument, where it builds its own copy.
  The path is resolved absolute now, and the `smoke` job passes a directory the way the
  release does, so the release's own invocation is exercised on every pull request.

- **The Kafka extra declared a driver floor that cannot be installed on two of the five
  Pythons this package claims.** `confluent-kafka>=2.3` against `requires-python = ">=3.10,
  <3.15"`, and that driver compiles librdkafka: measured on PyPI and then on real
  interpreters, the first wheel for Python 3.13 is **2.6.0** and the first for 3.14 is
  **2.12.1**, so `pip install confluent-kafka==2.6.0` on Python 3.14.6 goes looking for
  `librdkafka/rdkafka.h` and stops. Nobody met it, because a resolver takes the newest — but a
  lockfile pinning the floor, or `--resolution lowest-direct`, met it immediately.

  It is two specifiers now, by marker: `>=2.6` below Python 3.14 and `>=2.12.1` from it.
  Verified by resolving the lowest allowed version on three interpreters — 2.6.0 on 3.10 and
  3.13, 2.12.1 on 3.14 — and the API asks for nothing newer than the wheels do: 2.6.0 passes
  every Kafka case against a real broker, as does `pika==1.3.0`, the RabbitMQ floor, which
  nothing had run either.

  Every other floor was checked the same way and holds: `django==5.2.0`, `aiogram==3.30.0`,
  `redis==6.2.0` and `redis[hiredis]==6.2.0` all install on Python 3.14, and the redis floor is
  aiogram's own — `redis[hiredis]>=6.2.0` in its metadata.

  **Installation** states each transport's two floors in a table, because they are two
  questions and the page used to run them together: the *driver* is a package this project
  pins, the *server* is what you run. Redis Streams needs server 7.0 and refuses below it; the
  list runs on any server and gives at-least-once from 6.2, saying so in the log when it
  cannot. `tests/test_dependency_floors.py` ties the numbers together — the floors leg pins
  what `pyproject.toml` declares, the classifiers and the CI matrix cover exactly what
  `requires-python` allows, the three pages that state the Python range state the same one, and
  every extra's floor appears in the wiki. Five claims, each falsified.

- **The README says there are four transports, in the first screen.** It opened on one — a
  diagram with a Redis list in the middle, a single `pip install 'django-aiogram[redis]'`, and
  prose about pushing a call onto a list — so the page that decides whether anybody reads
  further described the default as the design. `BROKER` had been a setting for the whole
  release and the landing page never said so.

  It now names all four where the diagram was, with a table of what to write in `BROKER`, which
  extra installs the driver, and which settings that transport requires on top of the two in the
  block below it. Then the sentence that matters to somebody choosing: the code does not change
  with the row — same `bot.send()`, same handlers, same event log, same `manage.py start_tgbot` —
  what changes is what becomes of a message whose worker was killed mid-send.

  Three cases hold the table to the package: every transport in `broker/` has a row, every row's
  `BROKER` value imports and its extra exists in `pyproject.toml`, and the row marked default is
  the one `DEFAULTS['BROKER']` names. Resolving all four is also what pins the release's promise
  that no module reaches its driver at import time — measured on a venv with `redis` installed
  and neither `pika` nor `confluent-kafka`, where all four classes import.

  Two pages stated the requirements as though redis were still one: **Home** and
  **Installation** said `redis 6.2+` flatly, where the *server* floor now belongs to the
  transport — 6.2 for the list, 7.0 for Streams, and no Redis server at all for a project whose
  queue is RabbitMQ or Kafka. The driver is a separate question on those two, and the answer is
  usually still yes: `FSM_STORAGE` defaults to aiogram's Redis store.

  And the gate that exists for exactly this class of sentence — `test_broker_neutral_prose.py`,
  which pins the surfaces whose subject is the queue in general — could not see the one on the
  front page: it looks for Redis named as *the* queue, and "pushes the call onto a Redis list"
  matched none of its patterns. It does now, and four more surfaces joined the ones it covers,
  each with a sentence of this release's own to fix:

  * **Home**, the wiki's front page, read before any transport page.
  * **Serialization**, where the trust boundary was Redis's: *whoever can write to the queue*
    runs code in the bot container, and that is true on all four. Three sentences, including
    the one under `ALLOW_PICKLE`.
  * **AI assistants**, the brief handed to a coding agent — so the surface that propagates. It
    told one to watch `redis-cli llen TELEGRAM_BOT_MESSAGE` grow, which answers on one
    transport; `bot.queue_depth()` asks whichever `BROKER` names.
  * **Event log**, whose kinds table said `outbound.queued` meant "written to the Redis list"
    and `outbound.consumed` "took it off the list". Both fire on four transports.

- **`producer/client.py` is six modules.** It held the facade, the loop it drives, every
  route out of the process, the shutdown that has to hold the at-least-once guarantee
  across all of them, and — in the middle of that — fifteen one-line decorators and the
  helpers that name a send.

  What moved out says what it is: `outbound` (what names one send in flight and what
  settles it), `looping` (who may drive the loop and for how long), `queueing` (everything
  a queue write does except the write, plus the chunking that feeds it), `from_settings`
  (the aiogram objects a project's settings describe) and `routing` (the decorators, which
  read the router and nothing else). 1610 lines to 1195 at the split, and 1215 by the end of
  the release — the number a reader can count, since later entries here added to it.

  **The class is not cut further, and that is the decision.** The shutdown guarantee is a
  property of state that `send`, `_schedule`, `_drain` and `close` share; splitting it into
  mixins would scatter that state across files and leave none of them answerable for it.
  The decorators are the exception because they read one attribute and touch nothing else.

  Nothing a project names moved: `TelegramBot` and every method on it are where they were,
  including all fifteen handler decorators, which arrive through a mixin.

- **`eventlog/recorder.py` is five modules.** It held the queue, the writer thread, the
  shapes that cross between them, the settings the writer reads, two counters several
  threads touch and the fan-out to `events_recorded` — 922 lines in which the hardest part
  to reason about, the thread and its accounting, was the part with the least room of its
  own.

  What moved out says what it is: `records` (`Event`, `Wake`, `as_identifier` — the shapes,
  which travel further than the recorder does), `pacing` (the writer's numbers and the
  promise that reading one never raises on its thread), `bookkeeping` (the drop ledger and
  the thread marks, one lock each) and `publishing` (the fan-out, and the promise that it
  cannot raise). What stayed is the queue, the thread, the lifecycle around both and the
  gap row.

  **Import paths only, for a project that reached inside.** `Event` and `as_identifier` are
  `django_aiogram.eventlog.records` now, and nothing else a project would name has moved:
  `recorder`, `EventRecorder` and the `events_recorded` signal are where they were. The
  drop count and the thread marks are objects rather than attributes on the recorder,
  which makes the lock discipline structural — every path that changes a count is a method
  that takes the lock, instead of a `+=` somebody has to remember to guard.

- **Each setting belongs to one table: the package's, or the transport that reads it.** Four keys
  were in both, and the overlap made two rules quietly wrong. `W003` knows a key by the package-wide
  table plus whatever the configured transport declares, so `REDIS_MESSAGES_KEY` — being in the
  table — stayed known under Kafka, and a project that moved to Streams kept a line nothing reads
  without being told. `E007` validated that same key as a string on a deployment that has no list.

  `REDIS_MESSAGES_KEY` is the Redis list's alone now, and `BLPOP_TIMEOUT` is the package's alone:
  every consumer reads it, whichever queue it takes from, and it was declared by the two Redis
  transports as well. `REDIS_URL` and `REDIS_TIMEOUT` stay in both tables and mean it — the FSM
  storage builds a Redis client under every transport, so a Kafka deployment with
  `FSM_STORAGE = 'redis'` sets both and neither is stranded. That is also why `Broker.option`'s
  guard against declaring a *differing* default stays: it is what keeps one number in two tables
  honest.

  A rule follows its setting, and which kind a rule is needs no flag — a key outside the
  package-wide table belongs to whichever broker declares it, so a row guarding one runs only where
  that transport is configured. Nothing else changes for a project that keeps the Redis list.

  Two rules also stopped depending on the extra being installed: what a transport *declares* is
  class state and needs no driver, and asking with the driver verified collapsed to the package-wide
  table on a machine one `pip install` short — so `W003` called that transport's own required
  settings unknown keys and invited an operator to delete them, and every rule guarding one of those
  settings stopped running.

- **`DELIVERY` is a dotted path, and a consumer you write goes in it.** It accepted exactly one
  string until now — `'blpop'`, the name of a Redis command that three of the four transports
  never issue, since the consumer asks the broker and the broker reaches for `basic_get`, a
  stream read or a poll. So the setting documented one transport's mechanism while offering no
  choice at all. It resolves a path the way `BROKER` does; the default is
  `'django_aiogram.consumer.delivery.BlpopDelivery'`, so a project that changes nothing keeps the
  consumer it had.

  `Delivery.stopping` and `Delivery.read_timeout` are public with it, because a subclass that has
  to read `self._stop` to know when to return, or redo the read-deadline arithmetic to know how
  long it may block, is not being offered an extension point. The second is the one that bites:
  `BLPOP_TIMEOUT` capped by `HEARTBEAT_INTERVAL` and by `REDIS_TIMEOUT`, where a number of your own
  blocks for ever, raises inside the read, or lets the heartbeat expire under a consumer that is
  fine. That the socket term is `REDIS_TIMEOUT` on every transport is #41, filed and not fixed
  here; **Delivery** says so where a subclass reads the property. `Delivery` itself and any
  subclass that leaves `run()` abstract are refused by name rather than by `TypeError`.
  **Delivery** documents what `run()` must do, with the six rules that are each a defect this
  package has already had, and the page's own example is executed by the suite.

  `DeliveryKind` is **gone**: an enum of one member is not a choice. `E009` refuses `'blpop'` and
  `'keyspace'` by name and says what to write instead, rather than reporting that a Redis command
  is not a dotted path — and it stops at the *shape*, because resolving the path would import the
  consumer, which imports the serializer, which imports aiogram: 883ms and 135 MiB on every
  `manage.py check`, measured. `start_tgbot` settles the rest before it starts a thread, and
  `DeliveryNotConfiguredError` names the setting and the value when it cannot.

- **No transport driver is a dependency of this package any more.** `pip install
  django-aiogram` brings Django and aiogram; the driver comes with the transport you name.
  For the default that is **`pip install "django-aiogram[redis]"`** — an existing project
  upgrading from 3.x has to add the extra, or `redis` disappears with the rename.

  It is worth the one-time edit because the alternative is every deployment carrying every
  driver: a project on Kafka needs no Redis *for its queue*, and this is what lets the later
  transports ship without adding to what a base install downloads. It may still want
  `[redis]` beside its own extra — `FSM_STORAGE` defaults to aiogram's Redis store, and
  `E019` says so at startup — which is the one place a transport does not decide the whole
  install.

  Nothing guesses. `BROKER` names the transport, and a name whose driver is absent is
  `E047` at startup carrying that `pip install` line — with one exception that matters to a
  web container: a process with `ENABLED` off sends nothing, so it is not asked to install a
  driver, the same way `W002` does not ask a disabled process for a `REDIS_URL`. That gate is a
  trade rather than a proof -- the depth reads are not gated on `ENABLED`, so a disabled process
  that reads one does need the driver, and hears the `ModuleNotFoundError` the check exists to
  prevent. Firing here instead would warn every image build that never reads a depth. A `BROKER`
  naming something that is not a transport is still reported there.

  Ignore all of that and the send still says it in words. Measured on a base install with
  no driver:

  ```text
  from django_aiogram import bot        → ok
  bot.send(chat_id=1, text='hi')        → RedisListBroker needs the 'redis' package, which
                                          is not installed. Install it with:
                                          pip install "django-aiogram[redis]"
  ```

  The import working is the part that had to be built: **no module reaches its driver at
  import time**, so the package, the producer and a transport package all import on a
  machine that has no driver at all. Annotations moved under `TYPE_CHECKING` and a client
  is built inside the function that builds it — including aiogram's Redis FSM storage,
  which a project on `FSM_STORAGE = 'memory'` no longer pays for either.
- **The consumer and both producers talk to a transport, not to Redis.** What names an
  in-flight message is the transport's business — a payload for a Redis list, an entry id
  for a stream, a delivery tag, an offset — so a take hands back an opaque handle and it
  goes back unread. Nothing a project calls changed shape; `bot.send()`, `send_many`, the
  `await` twins and the depth reads all behave as they did.

  **If you patch the connection in tests, the name moved**: it is
  `django_aiogram.broker.redis_list.broker.get_redis`, because that is where the connection
  lives now. Patching the producer no longer reaches the write. See **Testing**.
- **Renamed throughout.** The distribution is `django-aiogram`, the import path is
  `django_aiogram`, the environment prefix is `DJANGO_AIOGRAM_`, the logger is
  `django_aiogram`, check ids read `django_aiogram.EXXX`, and the event log's table is
  `django_aiogram_event`. `TELEGRAM_BOT` stays as the settings key: it names what it
  configures rather than the package that reads it.
- **`src/` groups by what a thing is for**, so several import paths moved. The root keeps
  what cannot move — Django's `apps`, `models`, `admin` and `migrations`, and
  `python -m django_aiogram.healthcheck`, which a compose file runs — together with the
  five modules every cluster needs and none of them owns: `api`, `exceptions`, `context`,
  `_singleton` and `redis`. Everything else lives in `config/`, `producer/`, `consumer/`,
  `wire/`, `eventlog/` or `broker/`.

  Two moved paths are ones a project wrote down itself. `DATABASE_ROUTERS` becomes
  `django_aiogram.eventlog.dbrouter.TelegramEventLogRouter`, and the webhook view in your
  `urls.py` becomes `django_aiogram.consumer.webhook.telegram_webhook`. **`E048` catches the
  first**: it reports any `django_redis_aiogram.` entry in `DATABASE_ROUTERS` — naming the
  replacement for the router we shipped, and saying the distribution is gone for any other path
  from it, since a replacement it never had is not ours to invent. Otherwise the router is imported
  on the first query that needs routing — a request rather than a deployment step — and fails with
  Django's `ImportError`. It cannot catch the
  second, because nothing here can read your `urls.py`, so **Upgrading** carries that row and says
  what a stale one costs: a webhook answering 404 while the process looks healthy. `SERIALIZER` is
  unaffected — it holds a short name, not a path. **`DELIVERY` is a path now**, and the entry
  above says what to write; a 3.x value of `'blpop'` is refused by name rather than relocated.

  | was | is |
  | --- | --- |
  | `django_aiogram.settings`, `defaults`, `enums`, `checks` | `django_aiogram.config.*` |
  | `django_aiogram.client`, `throttling` | `django_aiogram.producer.*` |
  | `django_aiogram.delivery`, `webhook`, `routers` | `django_aiogram.consumer.*` |
  | `django_aiogram.serializers`, `envelope`, `payloads` | `django_aiogram.wire.*` |
  | `django_aiogram.recorder`, `events`, `instrumentation`, `signals`, `dbrouter` | `django_aiogram.eventlog.*` |
  | `django_aiogram.eventlog` | `django_aiogram.eventlog.writer` |

  The settings package is `config` and not `conf` for one reason worth writing down:
  `django_aiogram.conf` is the settings *object*, public since 2.x, and a subpackage at
  that path would shadow it.

## 3.1.0 - 2026-08-22

**At-least-once delivery is true now, and was not before.** A message is
acknowledged when its send finishes rather than when it is scheduled, so
duplicates after a crash are real where they used to be impossible — and losses
were. **Make your handlers idempotent on your own business key**, not on
`correlation_id`: a handler's replies inherit the id of the update that caused
them, so it is not one per message.

### Fixed

- **A refused send gives its in-flight slot back.** `MAX_IN_FLIGHT` bounds how many
  sends a consumer holds, and `send_raw` takes a slot before the send is scheduled —
  but its three refusal paths deliberately do not report completion, because the
  message was not sent and must stay in flight for a redelivery. So the slot was never
  returned: a handful of refusals during a shutdown, and the consumer stopped taking
  messages at all until it was restarted. `send_raw` takes an `on_refused` callback
  beside `on_complete` now, and the consumer passes both — the slot comes back, the
  message does not get acknowledged.
- **A replacement writer no longer strands the one it replaced.** `stop()` detaches
  the old queue and sets the stop flag; a `record()` landing next starts a
  replacement, and starting one *clears* that flag. The old writer then found an empty
  queue with nothing telling it to stop and waited on it for the life of the process,
  holding the database connection it had opened. It now leaves when its own buffer is
  no longer the recorder's queue, which nobody else can undo.
- **A receiver that turns the log off no longer strands the writer thread.**
  `events_recorded` receivers run on the writer's own thread, so one of them calling
  `recorder.stop()` is a reachable thing to do — and the writer then ran for the life of
  the process, holding a database connection. Its loop ended only when it had *seen* the
  wake `stop()` queues, and `stop()` drains that same buffer through `_abandon`, taking
  the wake with it; the flag and an empty queue say everything the wake said. `stop()`
  from the writer also stops trying to join itself, which it had been reporting as a
  writer that missed its deadline.
- **Logging an event could still destroy the caller's writes.** The guard that stops the
  log recycling a connection mid-transaction tested `in_atomic_block`, which is half of
  what an open transaction means — and the worse half. With autocommit off
  (`transaction.set_autocommit(False)`, or `AUTOCOMMIT: False` on the alias) the server
  holds one from the first statement and no block exists anywhere:
  `close_if_unusable_or_obsolete` then closes because `get_autocommit()` disagrees with
  the configured value, while `close()` skips `needs_rollback` *because* the block flag is
  False. So the caller's writes were rolled back by the server and nothing raised.
  Measured on PostgreSQL 16: the row was gone after a `commit()` that reported success.
  The suite could not see it — `tests/db_settings.py` is sqlite `:memory:` — so the test
  pins the rule rather than the consequence, with a control that still recycles a
  connection past its `CONN_MAX_AGE`.

- **A gap row the database refused took the gap with it.** `_record_gap` subtracted the
  count before writing and suppressed the write's failure, so the hole it could not
  describe was forgotten: no later flush would report those events and the feed read as
  complete coverage of a period that had lost rows.

  The count is now *claimed* under the counter's lock before the row is written, and given
  back if that row does not land — whether the write raised or the database refused the
  row on its own. Claiming rather than subtracting afterward is what keeps two flushes
  from reporting the same hole: `drain_once()` runs on the caller's thread while the
  writer runs its own, both snapshot the count before their batch, and a subtraction after
  the write let each of them take it off. It claims no more than is there, so a drop
  landing while the row is being written survives for the next flush.

- **Rows the database refused one at a time were counted nowhere.** The ladder under
  `write_batch` knew how many landed and used it only to decide whether to raise, so a
  batch of forty that lost one left the drop counter at zero with no `log.dropped` row —
  against this release's own promise that "what cannot is counted". `write_batch` reports
  the loss and the recorder counts it, which the next successful flush turns into a gap
  row.

- **`sortable_by` never stopped a sort.** Django reads it in one place, the template tag
  that decides whether a column header is a link, while `ChangeList` maps `?o=` straight
  onto `list_display`. A bookmark, a shared link or a query string kept from before the
  restriction still ordered the whole table by `function`, `worker` or `error_code`. The
  changelist drops an `?o=` naming a column no index can serve, and renders the page with
  the default ordering rather than refusing an old link.

- **The failure filter's bounded count was not bounded.** `BoundedPaginator` promised "one
  query the index can serve", and an `IN` over the seven failure kinds cannot yield a
  global `id DESC` from `(kind, -id)` — so the database sorted every match before the
  `LIMIT` could bite, which is the defect the index was added to remove, surviving in the
  filter that needs it most. The count is unordered now: which rows the cap admits does
  not change how many there are. Verified on the query plan, which names the index and no
  longer sorts.

- **`RAISE_EXCEPTION='false'` no longer re-raises.** `client.py` tested this setting with
  a bare `if`, the only boolean in the package still read that way, so the string
  `'false'` — which is what `DJANGO_REDIS_AIOGRAM_RAISE_EXCEPTION=false` arrives as, and
  truthy — propagated into the caller the exception the project had just asked to have
  swallowed. `'0'`, `'no'` and `'off'` did the same. Both reads now go through
  `coerce_bool`, behind one private property, so the two failure paths read the flag
  through the same line and cannot diverge. Private deliberately: the fix is the coercion,
  and a new name on the public surface would be a commitment carried into 4.0 for it.

  The failure needed a send to exhaust `MAX_RETRIES` first, so a project that spelled the
  flag the way an environment variable can spell it would have met it on the day Telegram
  started refusing them — not on the day they configured it. `E003` was strict for exactly
  this reason and is now `_a_readable_boolean` like the rest, which means **every boolean
  setting in this package is parsed rather than tested for truthiness**, with no exception
  left to remember.

- **Updates in one web process are handled concurrently.** A process serving the
  webhook drove nothing, so every `feed_update` took `run_until_complete` *under
  the loop lock* and updates handled strictly one at a time. Measured on four
  concurrent updates with a 200 ms handler: 0.81 s serialized against 0.21 s with
  the loop running. It also means a send scheduled from inside a handler now runs
  when it is scheduled — before, nothing stepped it until the next update
  arrived, or `close()`, or never.
- **`send_raw` no longer waits in a web process that serves the webhook.** It
  hands work to a running loop rather than driving one, and from the first update
  such a process handles there is a loop running. Measured: 0.30 s and Telegram
  called before it returns, against 0.00 s and Telegram not called yet. The
  practical difference is the exception — a send that fails after its retries
  raised into the view under `RAISE_EXCEPTION` and is now logged instead.
  A process that never serves the webhook is unaffected, and `send()`, which
  queues, never had this behavior. The Sending messages page says what to do
  when you need the answer.
- In webhook mode `start_tgbot` runs the loop instead of blocking on an event, so
  a send the consumer schedules runs when it is scheduled rather than waiting for
  whatever happens next. Both modes now start the consumer from the loop, which
  is what keeps a backlog from reaching `send_raw` while the loop is not running
  yet.
- `close()` waits for the updates a webhook process is still answering before it
  stops that loop, and cancels what outlasts `DRAIN_TIMEOUT`. A request thread
  waits on its update with no deadline of its own, so stopping the loop under one
  would hold that worker for the life of the process. A canceled update is
  answered 503, the same as one refused on arrival — nothing handled it either
  way, so Telegram should redeliver it rather than be told to forget it.
- **The webhook view answers 503 to an update it refused**, rather than 200. It
  answers 200 to a handler that raised, because retrying one that failed once is
  a loop — but an update refused mid-shutdown was never handled, so redelivery is
  the point. During a rolling restart that is the difference between the update
  moving to the next instance and disappearing.

- **The consumer acknowledged a message before Telegram had seen it.** In polling
  mode `send_raw` returns as soon as the coroutine is scheduled, and the consumer
  treated that as delivery: the message left the in-flight list at pop speed while
  Telegram is fed at `overall_per_second`. A `docker stop` with a backlog sent
  what the drain had time for — roughly 180 messages at the defaults — and lost
  the rest, with nothing left to redeliver, while the module docstring,
  **Delivery**, **Deployment** and **Troubleshooting** all promised
  at-least-once. Webhook mode always had the guarantee, because there the
  consumer drives the send to completion itself. A handler that accepts an
  `on_complete` keyword is now handed one and the message waits for it; one that
  does not — which is every documented recipe — keeps the old semantics exactly,
  so nothing outside this package changes behavior.

  The teardown settles those reports after the drain, which is the step that makes a
  *graceful* stop different from a kill: `close()` is what finishes the sends still in
  flight, and the loop that would have acknowledged them has already returned by then.
  Without it every message the drain delivered stayed in the in-flight list and the next
  start sent it again — a duplicate per restart, rather than per crash.
- **The webhook view answered 200 to updates nothing had handled.** `close()` puts
  `_closing` back to False in its `finally`, and `feed_update` checked only that flag and
  `loop.is_running()` — so a request that captured the loop before a teardown and reached
  the lock after it drove `run_until_complete` on a *closed* loop. The `RuntimeError`
  landed in the view's `except Exception`, which answers 200, and Telegram never
  redelivered the update. Measured under contention: 1,797 of them against 232,141
  correct refusals. `feed_update` now refuses a closed loop the way `_schedule` always
  has, so the view answers 503 and Telegram tries again.

  Three more from the same lens. `_ensure_loop_runs` would start a thread on a closed
  loop, where `run_forever` raises with nothing to catch it and every caller then waits
  out `RUNNER_TIMEOUT` for a readiness event no living thread can set. `_stop_runner` set
  `_runner` to None *before* it knew whether the join had worked, so a loop thread it
  could not stop was forgotten: `close()` says "leaving everything in place keeps
  `close()` retryable" while every later call returned immediately without asking the
  orphan again, holding the loop, the aiogram session and the FSM client for the life of
  the process. And it queued `loop.stop` even when the thread was already gone, which
  left the callback in the ready queue to fire inside the drain's own
  `run_until_complete` and take the teardown down with `Event loop stopped before Future
  completed`.

- **A non-ASCII secret header was an unauthenticated 500.** `hmac.compare_digest`
  refuses `str` arguments outside ASCII, so the comparison itself raised `TypeError`
  from the one branch whose job is to answer 403 — a traceback in the log for anyone who
  found the URL. Compared as bytes now.

- **A send arriving before our own loop thread reached `run_forever` killed it.**
  `is_running()` is False for that whole window, so `_schedule` drove the loop itself and
  the runner died with `This event loop is already running` — reported through
  `threading.excepthook` rather than the package logger. Worse, the driving call ran the
  `call_soon` the dead thread had queued, so `_runner_ready` was set and
  `_ensure_loop_runs` reported a runner it owned while nothing turned the loop; every
  update answered 503 until the next call noticed. `_schedule` now consults the runner as
  `feed_update` already did.

- **`manage.py check` no longer imports aiogram.** `DEFAULT_BOT_PROPERTIES`
  defaults to `{}`, and the check that validates it reached for
  `DefaultBotProperties` before noticing there was nothing to validate — so
  every `check`, and with it every `migrate`, `runserver` and `shell`, paid for
  aiogram in every project that installs this package. Measured on an
  installation that configures nothing: 966 ms and 174 MiB before, 7 ms and
  47 MiB after. A non-empty `DEFAULT_BOT_PROPERTIES` is still validated exactly
  as before.
- **The FSM storage is bounded by `REDIS_TIMEOUT` like everything else.** aiogram
  builds its own Redis client and was handed no connection settings, so it had no
  read deadline at all on the declared 6.2 floor, and redis-py 8's own five
  seconds rather than yours above it. Every update reads FSM state, so a Redis
  that accepts the connection and then stops answering wedged the whole bot
  instead of one send. A `?socket_timeout=` on `REDIS_URL` still wins, as it
  always did — redis-py resolves the URL after keyword arguments.
- `manage.py tgbot_*` drains by hand no longer fail on a Redis older than 6.2.
  `consume_pending()` had nothing to probe `LMOVE` support with — the consumer
  loop learns it from `reclaim()` — so the first pop raised `ResponseError`
  straight out of a documented helper instead of falling back to plain pops.
- **A send handed to the loop just before shutdown is no longer destroyed.** A
  hand-off is a `call_soon_threadsafe` callback until the loop steps, and a
  callback is not a task — so the drain could not see it, and `close()` had
  already set the flag its callback refuses on. The message was gone from the
  queue's in-flight list by then, so nothing would ever redeliver it. `close()`
  now runs one turn of the loop before draining, which is what turns those
  callbacks into tasks it can wait for — and counts itself as draining from the
  moment it begins, not only during that turn. A webhook process gave the loop a
  thread, so the callback ran on *that* thread while it was being stopped, in the
  window after the closing flag was set and before the drain claimed it: the
  coroutine was refused and closed there, one window over from where it was fixed.
- **The consumer's join deadline is derived from the bound that governs it.** It
  was `BLPOP_TIMEOUT + 1` — six seconds at the defaults — while every call the
  consumer makes is bounded by `REDIS_TIMEOUT`, ten. A consumer that outlived the
  join went on to acknowledge a message `close()` had already refused, destroying
  one message per shutdown. It is now `REDIS_TIMEOUT + 1`, and a thread still
  alive after it says so in the log instead of leaving silence that reads as a
  clean stop.
- **Recording an event no longer rolls back the transaction it was recorded in.**
  The writer discarded stale connections before every batch, and inside an
  `atomic()` block Django closes the connection on the first check — which sets
  `needs_rollback`. Under `EVENT_LOG_SYNC`, with `ATOMIC_REQUESTS` or a plain
  `atomic()`, that destroyed the caller's own writes on PostgreSQL and MySQL.
  The suite could not see it: sqlite `:memory:` refuses to close at all. Stale
  connections are still discarded, just never while a transaction is open — and
  only the log's own alias, never every connection in the process. With
  `EVENT_LOG_DATABASE` pointing somewhere of its own, the sweep reached past the
  log's connection, which is not in a transaction, to the caller's `default` one,
  which is.
- **A database that refuses everything is now reported as a failure.** The
  bisecting write caught `DatabaseError` at every rung and returned normally, so
  the writer counted a total refusal as a written batch: the suspension after
  repeated failures was unreachable, no `log.dropped` row was ever written, and
  a forgotten `migrate` meant a full batch of statements and tracebacks once per
  flush interval, for ever.
- **Events are no longer lost when the writer thread dies or when `stop()`
  races a `record()`.** Both left a queue nobody would ever drain again, and its
  contents disappeared with no row, no counter and no gap marker. Both now drain
  it: what can be written is written, and what cannot is counted, so the next
  successful flush records the gap.
- The writer reads its own batch size, buffer size and flush interval with a
  fallback to their defaults. They are read on the writer thread, in a loop,
  outside the net that contains a failed flush — so a value that could not be
  parsed ended the writer and took the whole buffer with it. Checks `E036`–`E038`
  still report it at boot.
- **The changelist's bounded count is now actually bounded.** The kind index was
  `(kind, -created_at)` while the changelist orders by `-id`, so every filtered
  page sorted in a temporary b-tree — the page query and the count alike. A
  `LIMIT` can only stop early if the rows arrive ordered, so the documented
  "counts at most 10 000 rows" was not true for any filtered view. The index is
  `(kind, -id)` now. **This release ships migration `0002`; run `manage.py
  migrate`.** The new index is added before the old one is dropped, so the table
  is never without one on `kind`, and the name is new because the columns differ.
  `AddIndex` issues a plain `CREATE INDEX`, so on a table large enough for the
  lock to matter, run the migration in a window. What it costs: a per-kind *time
  window* query loses its range column. `drai_event_recent` still covers a time
  window without a kind.
- The changelist stops fetching `error` and `detail`. It renders neither, and
  between them they are most of what a row weighs. Under `EVENT_LOG_PAYLOAD: 'full'`
  with long tracebacks that is about 1.4 MB per fifty-row page; on the default,
  `'summary'` with its 8 KiB cap, far less — either way fetched to be discarded, and
  fetched even for a reader the payload permission withholds them from.
  The detail page asks for them back, and only for a reader allowed to see them.
- Only indexed columns are sortable in the admin. One click on the `function`,
  `worker` or `error_code` header was a full sort of a table sized by traffic.
- **`tgbot_prune_events` makes forward progress past a recent low-id row.** The
  walk's lower bound was the table's lowest id while its upper bound was filtered
  by the cutoff, so one surviving row down there pinned every run to restart from
  it — and a bounded `--max-chunks` run spent its budget crossing rows it could
  not delete. The watermark is also read with `ORDER BY id DESC LIMIT 1` instead
  of an aggregate, and the pause between chunks is skipped when a chunk deleted
  nothing and under `--dry-run` entirely.

- **The boolean checks no longer refuse configuration that works.**
  `{'ENABLED': 'true', 'EVENT_LOG': '1', 'ALLOW_PICKLE': 'no'}` is documented, boots
  and sends — and failed `manage.py check` on one id per setting it names, because the
  rules demanded a real `bool` while every one of those settings is coerced where it is
  used. They were
  inverted twice: the values `coerce_bool` genuinely refuses raise
  `ImproperlyConfigured` out of `apps.ready()` before a check runs, so the errors could
  never fire on the case they were written for. E001, E002, E017, E031, E042 and E046
  now ask by trying the coercion and report the message the runtime would have raised.
  `E003` went with them once `client.py` stopped reading `RAISE_EXCEPTION` on raw
  truthiness — see the entry above — so no boolean in this package is tested for
  truthiness any more, and there is no exception left to remember.
- **`W004` compared `BLPOP_TIMEOUT` against the wrong bound.** The consumer caps its
  pop at `min(BLPOP_TIMEOUT, HEARTBEAT_INTERVAL, REDIS_TIMEOUT - 1)`, and the rule
  looked only at the deadline — so `BLPOP_TIMEOUT=30, HEARTBEAT_INTERVAL=10,
  REDIS_TIMEOUT=60` was silent while the pop ran at ten, and when the warning did fire
  it told the operator to raise `REDIS_TIMEOUT` whether or not that was the term
  binding. Both the check and the consumer now read one helper, and the hint names
  whichever setting produced the cap.
- **The container healthcheck no longer builds a bot.** It imported the shared `bot`
  for one flag and a `Delivery` for a branch that cannot fire — `crash_safe` starts
  true and is only lowered by `reclaim()`, which the probe must never call. Both pull
  aiogram: 902 ms against 16 ms with `AUTODISCOVER=0`. It reads the keys from the
  module that owns them and asks the server about the guarantee, which the rest of
  that method already did.

- **The container healthcheck could not finish inside any timeout the wiki could
  publish.** `manage.py tgbot_healthcheck` was correct and unusable: a management
  command runs `django.setup()` first, which populates the app registry and executes
  every `AppConfig.ready()` in the *host* project before the probe reads a single key.
  Measured in a consumer project — Django 5.2, twenty apps, one registering adapters in
  `ready()` — 2.45s for the settings module, **17.89s** more for `apps.populate()`, and
  ~0.01s for the three Redis calls the probe actually makes. Docker killed it at the
  documented `timeout: 10s`: `ExitCode: -1`, `FailingStreak: 62`, while the probe's own
  last line read `healthy: heartbeat 6s old, 0 queued`. The container reported unhealthy
  for the best part of an hour with nothing wrong.

  The decision now lives in `django_redis_aiogram.healthcheck`, which imports nothing
  that needs the registry, and **`python -m django_redis_aiogram.healthcheck`** is what
  a healthcheck should run: 69 ms end to end, interpreter startup included.

  **What a consumer does:** change the `test:` line, put `DJANGO_SETTINGS_MODULE` in the
  container's `environment:`, and lower `timeout:` if you had raised it. That variable is
  the one thing this form needs and the command does not: `manage.py` sets it with
  `os.environ.setdefault(...)` *inside its own process*, so a container that runs
  `manage.py` may never export it, and a healthcheck is a different process. Without it
  the probe writes `cannot read the settings: …` and exits 1.

  `manage.py tgbot_healthcheck` keeps working — it is a wrapper now, with the same flags
  and the same exit codes. It also keeps scanning for stranded in-flight lists and
  reporting the delivery guarantee, which the `python -m` form leaves off behind
  `--stranded` and `--guarantee`: neither can change the verdict, the scan is up to twenty
  `SCAN` rounds over a keyspace often shared with a cache, and the guarantee probe is a
  write that answers `unknown` on a read-only replica. Twice a minute for a line nobody
  reads was the wrong trade.

  Two lines it prints did change, and both are worth knowing if you read them by hand or
  grep them. A sweep that fails partway now reports the count it did reach as
  `at least N message(s) are in flight …` where it used to print the healthy line alone —
  which reads as *none*, the one conclusion a partial sweep cannot support. And a missing
  heartbeat now names the limit the probe judged by, so `--max-age 600` no longer says
  `within 30s`; the `--help` text of both limits is reworded to match between the two
  forms, which now declare them from one place.

  `ping`, `GET` and `LLEN` are now guarded by `except RedisError` rather than
  `except Exception`, which is what the two later stages already used. That narrowing
  showed the suite's own fakes were raising Python's built-in `ConnectionError` — which
  no real client produces, since redis-py's subclasses `RedisError` and not the
  built-in one. It also had to keep three non-Redis failures readable rather than let
  them out as tracebacks: an empty `REDIS_URL` (`ImproperlyConfigured`), a `REDIS_URL`
  with no scheme or an unreadable `REDIS_TIMEOUT` (`ValueError`), and a heartbeat that
  cannot be decoded, which `decode_responses` in a URL shared with a cache backend makes
  possible (`UnicodeDecodeError`) — that one can also come off a foreign key the stranded
  sweep matches, where it now leaves a warning instead of aborting the probe. A mistyped
  `DJANGO_SETTINGS_MODULE` is answered the same way as a missing one; an import the
  settings module itself fails at keeps its traceback, because that fault is not this
  package's to flatten into a line.

### Added

- **`python -m django_redis_aiogram.healthcheck`**, and `--stranded` / `--guarantee` on
  it, so a container probe can answer without booting Django. See the `### Fixed` entry
  above for why the management command could not be used in a healthcheck at all.

- **`await bot.asend(...)`**, and `asend_redis`, for code already on an event
  loop. `send()` writes to a socket on the calling thread, which under ASGI is
  the thread serving requests, and on a first call that includes a TCP connect
  bounded by `REDIS_TIMEOUT`. Native `redis.asyncio` rather than a thread wrapper:
  `sync_to_async` measured the same for one send, but its default
  `thread_sensitive=True` runs every call in one shared thread — twenty
  concurrent sends took 0.49 s against 0.02 s — and even threaded it draws on a
  pool shared with every other `sync_to_async` in the process, the ORM's
  included. Each loop gets its own client, because these connections are
  loop-affine, and `await bot.aclose()` is the only way to close one on the loop
  that owns it — the only loop permitted to. A process that runs a loop per unit of
  work should call it; the registry drops clients whose loop has closed, so nothing
  accumulates without it, but the sockets wait for the collector. **Deployment**
  has the recipe.
- **`bot.send_many(chat_ids, ...)`** and **`await bot.asend_many(...)`** queue one
  message per chat, a chunk of them per variadic `RPUSH`, returning an id per
  message in the order given. It speeds up *queueing*, not delivery — the rate
  limits still pace what leaves, so fifty thousand chats is about half an hour at
  the default thirty a second — and it makes event-log overflow worse by removing
  the pacing sequential round trips gave the writer. A chunk that fails records a
  drop for its own messages and raises; the earlier chunks are already queued.
- **`bot.queue_depth()`** and **`bot.inflight_depth(worker=None)`**, with async
  twins, so a monitor stops reproducing `<REDIS_MESSAGES_KEY>:processing:<worker>`
  by hand — a scheme that is this package's to change. Troubleshooting used to
  send people to `redis-cli` for it.
- Both producers now share one write body: serialization, the key and both event
  rows live in a single context manager, and each transport is the one line that
  writes. The `await` is the only thing the two cannot share, so it is the only
  thing they do not.

- **`manage.py tgbot_reclaim --worker <name>`** puts a dead worker's in-flight
  messages back on the queue. Crash safety rests on a restarted worker
  recognizing its own list, and a container started without `hostname:` gets a
  fresh name from Docker for each container it creates — so every replacement,
  which is what a redeploy does, stranded whatever the last one was sending where
  nothing would look again. The command is deliberately
  manual: naming a worker is a human saying it is gone, and one that is merely
  slow looks exactly like one that is dead. `--dry-run` reports without moving
  anything, through the same `--limit` a real run applies, and `--limit` bounds
  what one run can move.
- Check `I001` reports the case it can detect: `WORKER_NAME` empty while the
  hostname is one Docker generated. Narrow on purpose — an unset `WORKER_NAME` is
  the documented default and correct almost everywhere, so warning about it as
  such would fire on every untouched installation and teach people to stop
  reading warnings. Information rather than a warning because a check cannot tell
  a consumer from a web process, and only the consumer is affected; `start_tgbot`
  warns for itself, where the process is known.
- `manage.py tgbot_healthcheck` says which guarantee is in force — established
  by asking, not assumed from a default — and how many messages sit in flight
  under other worker names, so a stranded pile stops being invisible. It reports
  rather than acts: one of those may be a message another worker is sending this
  second. The count is a floor and says so when it is one: `SCAN` walks the whole
  keyspace, and this runs on a healthcheck timer, so the sweep is bounded.
- `MAX_IN_FLIGHT` bounds how many sends the consumer will leave outstanding
  before it stops taking messages, with `0` — the default — meaning no bound.
  Worth setting on a worker that sees large backlogs: acknowledging is an `LREM`,
  which scans the in-flight list, so an unbounded one turns draining a backlog
  into quadratic work.
- `REQUIRE_CRASH_SAFE` refuses to start where `LMOVE` is missing, rather than
  running at-most-once and only saying so in a log line. Checked before the
  consumer thread starts: a failure raised inside it would kill the thread and
  leave the process polling updates with nothing draining the queue. An
  unreachable Redis is not mistaken for an old server.
- `Delivery.crash_safe` reports which guarantee is actually in force.
- Checks `E045` and `E046` for the two settings above.
- `DRAIN_TIMEOUT` sets how long `close()` gives in-flight sends before canceling
  them. It was hardcoded at five seconds and `start_tgbot` called `close()` bare,
  so a deployment could raise `stop_grace_period` all it liked and never buy the
  drain a second more. The Deployment page now has the arithmetic for sizing the
  grace period against all three waits.
- Check `E044` refuses a `DRAIN_TIMEOUT` that is not a finite number, or is
  negative. `close()` reads it while shutting down, between stopping the consumer
  and flushing the event log, so raising there would cost the rows describing what
  the drain just did — it falls back to the default rather than refusing, and the
  check is what tells you at boot.
- Check `E043` refuses a `REDIS_URL` that sets `decode_responses` while
  `ALLOW_PICKLE` is on. Decoding is otherwise supported and stays supported — one
  URL is often shared with a cache backend — but a pickled payload is not valid
  text, and redis-py decodes inside its own parser: the consumer raises *after*
  the server has moved the message to the in-flight list, and each later start
  trips over that message once before carrying on — measured on Redis 8, one error
  per restart, then the queue drains around it. An error rather than a warning
  because the message itself is unreadable for ever and only a hand-deleted key
  clears it, but not a wedged consumer: an operator who reads this as a dead queue
  drains it by hand for nothing.
- **A metrics seam that is not the event log.** `django_redis_aiogram.signals`
  carries `events_recorded`, a `django.dispatch.Signal` fired once per batch on the
  event writer's own thread, with the `Event` objects that batch holds — except in the
  two cases where there is no writer thread to run on: under `EVENT_LOG_SYNC`, which
  only takes effect with the log on, and at shutdown for whatever the writer had not
  drained, on the thread that called `recorder.stop()`. A signal
  rather than a setting naming a dotted path: no path to get wrong, no check id for
  it, no lazy import cache, and no question about what a failing import means.
  A receiver that raises costs neither the other receivers their batch nor the
  database its rows, and is logged as `an events_recorded receiver raised`.
  `send_robust` is most of that — and only most: Django's own failure logging reads
  `receiver.__qualname__`, which a *callable instance* does not have, so for that
  shape `send_robust` raises instead of containing anything, measured on 6.1. That is
  caught here as well and logged as `publishing recorded events failed`, because
  otherwise it reached the writer's failure counter and was reported as a database
  refusing a batch it never saw.

  It fires with `EVENT_LOG` **off** — the table and the metrics are separate
  decisions, and gating them together is how an advertised metric comes out
  silently empty. So the one gate became three: `enabled` still means "this process
  writes rows", `active` means "the table or a receiver is reading" and is what
  every producing seam now sits behind, and `wants_payload` guards only the
  summarizing, which is the expensive part and no part of counting — so with the log
  off a receiver gets `Event` objects without the *summarized arguments*, while still
  getting what the seam measured itself: a send's `duration_ms`, a retry's
  `retry_after`, a queueing failure's `stage`, a gap's `dropped` count. Rows are what
  the table gets, and with the log off there are none. `EVENT_LOG_KINDS` filters
  receivers as well, because it is one answer to "which events does this deployment
  care about" and not two — except for `log.dropped`, which is the record that
  recording itself fell behind and is exempt in both directions, since a deployment
  that filtered it out would read the hole as quiet traffic.

  Receivers see the batch **after** its write has been attempted — and only attempted:
  with the log off there is nothing to write, and a failed write publishes anyway. They
  get it as a tuple.

  Both are containment rather than convenience. Publishing came *first* originally, and
  receivers were handed the same list and the same `Event` objects the ORM was about to
  read — and a frozen dataclass does not freeze the `detail` dict inside it, so a
  receiver clearing the list or editing a `detail` changed what got persisted. It
  cannot now: by the time a receiver runs, the write is done. The tuple covers what
  ordering does not — `send_robust` hands every receiver the same argument, so one of
  them could otherwise decide what the next one sees. Each `detail` dict is still
  shared between receivers, so treat it as read-only.

  `Event`'s field names are pinned in `tests/test_public_surface.py`, which makes
  them public API. Importing the seam pulls neither aiogram nor the ORM: **0.15 ms**
  on top of a process that already has `django.dispatch`, which every Django process
  has by the time settings are read. From a bare interpreter it is 16 ms, almost all
  of it `django.dispatch` pulling `asgiref` — which is why the test asserts what gets
  imported rather than how long it takes. And a process that has receivers but no table no longer imports
  `eventlog` — and so `django.db` — to close a connection it never opened.
  **Event log** has the recipe, including the two honest notes about
  `prometheus_client` and about which container has to run the exporter.

- **Check `I002`** — `EVENT_LOG_DATABASE` names an alias with nothing in
  `DATABASE_ROUTERS` routing this app there. E040 sees a string, E041 sees a
  configured alias with a real engine, W005 sees a database, and `migrate` still never
  creates the table: the writer logs `no such table` once per batch for ever.
  Information rather than an error or a warning, because a router of your own returning
  the same alias is a legitimate way to do it and this cannot see inside one. Compared
  through `import_string`, so a dotted path and an instance both count.

### Changed

- **Encoding a queued call takes one pass instead of two.** `encode()` rebuilt
  every container and `json.dumps` then walked the copy. A `JSONEncoder` that
  tags as it writes produces the same bytes from one walk: a plain send 2.17 →
  0.58 µs, an envelope 4.12 → 0.98 µs, a thirty-button keyboard of **plain dicts**
  53.3 → 6.0 µs. Built from `InlineKeyboardButton` objects, which is how every
  keyboard on **Sending messages** is built, the same markup costs 235 µs — the
  encoder still walks each model through the recursive path, and that is where the
  time goes rather than in the JSON.
  A payload built from aiogram model objects is unchanged at 1.0x — `ModelCodec`
  still recurses per field, and it has to, because `encode` is exported and a
  codec returning half-tagged data would break every caller that uses it alone.
  A payload too deeply nested to read back is still refused: the C encoder
  ignores Python's recursion limit while `decode` does not, so without a guard
  such a call would be queued happily and then be undecodable for ever.
- **Redaction reads the settings once per payload, not once per string.**
  `redact_text` resolved `TOKEN` and `WEBHOOK_SECRET` for every string at every
  depth of every event, and `to_row` rebuilt the redaction key set for every row
  of a two-hundred-row batch. Both are hoisted. A string with no colon also skips
  the token regex, which is exact — every token Telegram issues contains one —
  and covered by its own test, because what it guards is the token reaching a row.
- **The rate limiter no longer spins.** It paced correctly, but by counting
  tokens: every waiter recomputed the same wait from the same shared state, so N
  waiters woke together, one won and the rest went back to sleep — about N²/2
  wakeups, which is the shape rather than a number, because the old design is gone
  and cannot be re-measured honestly. What is measured is what ships: **35 wakeups
  for 40 queued sends, 495 for 500** — one per send that had to wait, since the burst
  is admitted without sleeping at all, so the count is `N - capacity`. A
  recompute-and-re-sleep tail costs 120 251 for those same 40, which is what the
  test that pins this uses to fail. Admission also becomes strict FIFO —
  before, a herd re-racing for the same token admitted in whatever order the loop
  happened to resume, so the message that had waited longest had no claim on
  going first. The limits themselves are unchanged, and every existing pacing
  test passes untouched.
- Redis commands are documented as deliberately un-retried. `Redis.from_url`
  builds the pool before the client, so redis-py's client-level retry default
  never reached the connection and every command already ran with zero retries;
  that was an accident, and it is now a decision with a test behind it. Neither
  `RPUSH` nor `BLMOVE` is idempotent, and redis-py retries the whole
  send-and-read — a connection dropped after the server applied an `RPUSH` would
  queue the message twice and a real person would receive two of them.

- **Importing the package costs 0.134 ms instead of 1.420 ms**, and pulls neither
  `typing` nor `threading`. Annotations are postponed and `TYPE_CHECKING` is a local
  sentinel; the lock around building the shared bot is gone, because a module body
  runs once per process under Python's own import lock. Honestly: this only matters
  outside Django, where `django.setup()` has not already paid for both modules —
  inside one the figure is 0.088 ms either way. The guarantee the lock used to give is
  pinned by eight threads on a barrier asserting they get one instance.
- **`orjson` is not coming, and here is the number.** On a fixed 202-byte send,
  `timeit` over 200 000 calls on CPython 3.13.14: `serializer.dumps(payload)` is
  **0.98 µs** with the serializer bound the way the queueing path binds it, and a bare
  `json.dumps` producing the same bytes is **0.83 µs**. So the tagging costs about
  0.08 µs, and a faster library has to beat that plus the 0.83 µs underneath it —
  roughly a microsecond in total, against a Redis round trip of 14 µs on Linux — 105 µs
  measured on macOS, so read it as an order of magnitude rather than a constant — and a
  Telegram call
  in tens of milliseconds. Resolving the serializer is a separate 0.09 µs, paid once per
  write rather than per message, and worth separating because it is the same size as the
  overhead. `orjson` would also change what is representable, since the tagging depends
  on `default` being called for exactly the types it registers. The runnable bench, the
  payload and the platform are on the Serialization page so the question stops being
  reopened.

### Infrastructure

- **`pip install -e '.[dev]'` becomes `pip install -e . --group dev`.** The dev
  requirements were an extra, so `Provides-Extra: dev` shipped in the wheel and
  every consumer resolving the package saw a group of linters they have no use
  for. They are a PEP 735 dependency group now, which needs pip 25.1 or newer —
  `AGENTS.md`, `CONTRIBUTING.md` and CI all say the new command.
- An optional `hiredis` extra: `pip install django-redis-aiogram[hiredis]` makes
  redis-py parse the protocol in C. Worth it on a busy consumer, pointless on a
  web tier that only ever pushes, which is why it is opt-in rather than a
  dependency.
- The sdist carries `docs/`, `scripts/`, `CONTRIBUTING.md`, `SECURITY.md` and
  `AGENTS.md`, and the wheel gains the `Framework :: AsyncIO`,
  `Framework :: Django :: 6.1` and `Topic :: Communications :: Chat`
  classifiers, plus `Repository` and `Funding` URLs.
- `redis_conn` is annotated for consumers. It forwards through `__getattr__`, so
  `redis_conn.ping()` typed as `Any` while `get_redis().ping()` did not; the
  smoke install now `assert_type`s it, which is the only place a packaging-level
  typing regression is catchable.
- CI gains a Django 6.1 leg and a `valkey/valkey:8` integration leg — the fork
  most managed providers actually run. The integration suite also asserts that an
  unknown command's error text contains `unknown command`, which is the single
  assumption the crash-safety downgrade rests on and which fakeredis can never
  answer.
- `publish.yml` checks that the release tag and `__version__` agree before
  building, and pins every action it runs to a commit — it is the one workflow
  holding `id-token: write`. The tag reaches the shell through `env` rather than
  template interpolation, since a tag may legally contain a quote, and the build
  tools are installed by hash from `.github/release-requirements.txt`, with
  `.github/release-constraints.txt` pinning the backend that `python -m build`
  resolves in an isolated environment of its own. That job is where third-party
  code last touches the artefact PyPI receives. `hatchling>=1.27` in
  `pyproject.toml` is unchanged: only what CI installs is pinned, not what
  consumers build against.
- Deprecation warnings fail the suite. Deliberately not a bare `error`: that
  escalates `ResourceWarning` into `PytestUnraisableExceptionWarning`, whose
  attribution follows GC timing and differs across the 3.10-3.14 legs.
- The lazy-boot tests assert what they were meant to. The first now compares a
  `sys.modules` delta against `sys.stdlib_module_names`, so it catches any
  third-party import the package pulls rather than aiogram alone, and a third
  test covers the checks. They run under `tests/bare_settings.py`, because the
  suite's usual settings install an app whose router imports aiogram anyway.
- `django-stubs` is held below 6.1. 6.1.0 resolves every `Self`-returning
  `QuerySet` method to `Any`, which fails `mypy --strict` on code that has not
  changed since 3.0.0.

### Documentation

- **A `TELEGRAM_BOT` that is empty and not a mapping is refused rather than ignored.**
  Found while writing the docstring for `_resolve`, which claimed the opposite of what the
  code did. `getattr(django_settings, 'TELEGRAM_BOT', None) or {}` folded every falsy value
  into an empty mapping *before* the check meant to catch a wrong type, so `[]`, `()`, `''`
  and `0` reached none of it: the setting was silently discarded and every value — the token
  included — came from the environment or the defaults. A project that had configured the
  bot then ran as though it had not. Only `None` and an absent setting mean *not
  configured* now. The two tests that covered this used `['not', 'a', 'mapping']` and
  `'TOKEN=abc'`, both truthy, which is how the hole survived three releases.

- **Everything in `src/` has a docstring, and a test keeps it that way.** Eighteen
  definitions were missing one, and they were not an even scattering: every single one
  was a nested closure or a private helper, which is exactly what ruff cannot ask about.
  `select = ["ALL"]` has required docstrings since 2.0, but pydocstyle's rules apply to
  *public* module-level and class-level definitions, so `ruff check` was silent on the
  retry loop inside `send_raw`, on the done-callback that decides whether a killed send
  is acknowledged, on the thread body that owns the event loop, and on the latch that
  keeps one message from being reported finished twice. The most load-bearing code in the
  package is written as closures. Verified rather than assumed: deleting one of the new
  docstrings leaves `ruff check src` reporting *All checks passed!* while
  `tests/test_docstring_coverage.py` fails and names the definition.

  That test walks the syntax tree, descends into function bodies — and into `try`, `with`
  and `if TYPE_CHECKING`, where a definition can also hide — and reports `line:qualified.name`
  rather than a percentage, because one number over a threshold does not say which
  definition is missing. Its own control asserts that a nested definition *is* seen, since
  a walker that stopped descending would report 100% for ever. A second check refuses the
  degenerate restatement — a summary whose every word is filler or a word of the name, so
  `def _bucket(): """Return the bucket."""` fails — run against all 484 definitions
  in `src/` and reporting none of them, because a false positive there would fail the build
  on a docstring somebody wrote on purpose.

- **The review's docstring check now asks about the library rather than about test
  naming.** It had been the one failing pre-merge warning for most of this release, at
  61.83% across the repository — a figure produced by ~576 undocumented definitions under
  `tests/`, nearly all of them two-line fakes inside test bodies, where `pyproject.toml`
  ignores `D` deliberately: *test names are the documentation*. Reaching 80% that way
  would have meant padding the suite to satisfy a number, and overruling a decision this
  project took on purpose.

  The check cannot be scoped: its schema offers `mode` and `threshold` and nothing else,
  and `reviews.path_filters` drives a sparse checkout, so excluding `tests/` would take
  them out of review entirely — and review of the tests is what caught three assertions
  that could not fail, in this release alone. So the built-in check is off and a custom
  check asks the same question about `src/`, with the reason in `.coderabbit.yaml` beside
  it. `mode: 'off'` is quoted there because YAML 1.1 reads a bare `off` as the boolean
  false, which the schema rejects — an invalid config is an ignored config, and the 80%
  would have come straight back.

## 3.0.0 - 2026-08-09

Two kinds of change at once: the compatibility 2.0 shipped for 1.x is gone —
the `telegram_bot` package name, `keyspace` delivery, and the string constants
that aliased enum members — and the package can now record what it did to a
table. The removals are mechanical and `manage.py check` names each one. The
event log is opt-in and off by default.

One piece of compatibility is **added** rather than removed: the consumer reads
both the new envelope and the flat payload 2.x wrote, so a backlog drains
across the upgrade.

**Upgrade the bot container before the web tier.** Queued payloads now carry an
envelope, and a 2.x consumer handed one loses the message.

**Run `manage.py migrate`.** The package ships a table for the first time,
created whether or not you turn the log on.

### Breaking

- **The `telegram_bot` package is gone.** 2.0 kept it as a deprecated shim and
  said it would be removed in 3.0; this is that. Put `django_redis_aiogram` in
  `INSTALLED_APPS` and import from it — `TelegramBot` is in
  `django_redis_aiogram.client`, the settings module is
  `django_redis_aiogram.settings`, and the management commands keep their names.
  A project that upgrades without touching `INSTALLED_APPS` fails at startup with
  `ModuleNotFoundError: No module named 'telegram_bot'`, which is the loudest
  this could reasonably be.
- **`keyspace` delivery is gone**, and with it `REDIS_EXP_KEY` and
  `REDIS_EXP_TIME`. It reproduced the 1.x mechanism — write a key with a TTL and
  react to its expiry event — and needed `CONFIG SET notify-keyspace-events`,
  which managed Redis providers refuse; it also could not deliver anything
  before the TTL elapsed. `blpop` has been the default since 2.0, needs no
  server configuration and delivers as the message arrives. Remove
  `'DELIVERY': 'keyspace'` from your settings: the value is now refused by check
  `E009` rather than silently ignored, so `manage.py check` tells you before the
  worker starts. Checks `E008` and `E013` are gone with the settings they
  guarded, and their ids are not reused — a `SILENCED_SYSTEM_CHECKS` entry
  naming one is now dead but harmless.
- **Queued payloads carry an envelope.** 2.x wrote
  `{'function': name, **kwargs}` and the consumer splatted it straight back into
  aiogram, so there was nowhere to put a correlation id without it arriving at
  Telegram as an unexpected argument. 3.0 nests the arguments under
  `__envelope__` instead. The reader accepts the old flat shape, so a backlog
  drains — but the reverse does not hold: a 2.x consumer handed a new payload
  calls the Telegram method with `__envelope__` as a keyword, raises, logs it
  and swallows it, and the message is lost. **Deploy the bot container before
  the web tier.** A test reproduces the failure so the constraint is not
  folklore.
- `bot.send()`, `send_redis()` and `send_raw()` return the correlation id
  instead of `None`, and take one as a keyword argument. Source-compatible at
  every existing call site. A handler replying to an update inherits that
  update's id through a context variable, so a reply is joined to its cause
  without any project code passing it along.
- **Inbound updates and FSM transitions are recorded too.** One outer
  middleware on the dispatcher sees every update exactly once, whether it
  arrived by polling or by webhook, and records when it arrived, how long the
  handlers took and whether they raised. FSM transitions come from wrapping the
  configured storage: `set_state` *is* the transition, so it costs no extra
  round trip and misses none of the ones a filter or a scene makes.
- With the log off, no middleware is registered and the storage is handed back
  unwrapped — the cost per update is zero rather than one branch.
- `EVENT_LOG_SYNC` falls back to the writer thread inside a running loop. The
  ORM is unusable there, so writing on the calling thread would turn the update
  middleware from a recorder into a source of `SynchronousOnlyOperation`.
- Asserting on the queue in your own tests now goes through
  `envelope.unpack` — see **Testing** in the wiki, whose recipes are executed
  by the suite and were updated with the format.
- `DeliveryKind` has one member, `BLPOP`. `DELIVERY` stays as a setting so a
  stale `'keyspace'` produces an error naming the legal value, rather than an
  unknown-key warning and a silently different delivery mode.
- **The 2.0-spelling string constants are gone.** `BLPOP_DELIVERY`,
  `KEYSPACE_DELIVERY`, `MEMORY_STORAGE`, `REDIS_STORAGE`, `JSON_SERIALIZER`,
  `PICKLE_SERIALIZER`, the `TAG_*` names, `OVERALL_PER_SECOND`,
  `PER_CHAT_PER_SECOND`, `GROUP_PER_MINUTE`, `POLLING` and `WEBHOOK` were
  aliases of the enum members carrying the same strings, kept because 2.0 had
  shipped them under those names. Import the member instead —
  `SerializerKind.JSON`, `SerializationTag.MODEL`, `UpdateMode.WEBHOOK` — and
  interpolate `.value`, never the member: a `(str, Enum)` member formats as its
  own qualified name on newer Pythons. The values are unchanged, so nothing in
  Redis or in your settings has to move.

### Added

- **An event log.** `TELEGRAM_BOT['EVENT_LOG'] = True` records what the package
  did — a message queued, delivered, failed or retried, an update received, an
  FSM transition, a payload refused — as rows in one append-only table. Rows for
  the same message share a `correlation_id`, so the row a web process wrote when
  it queued the send lines up with the row the bot container wrote when it
  delivered it, across two processes, with no coordination and no foreign key
  either way. See **Event log** in the wiki.
- Writes go through one background thread that batches with `bulk_create`, so no
  send ever waits on the database. A database that is slow or down costs dropped
  rows, never dropped messages; the drop is logged and, once the writer catches
  up, recorded as a `log.dropped` row so the gap is visible in the data too.
- `EVENT_LOG_KINDS` selects which kinds to keep, and `events.register_kind` adds
  your own. `kind` is an unconstrained column and the registry lives in Python,
  so adding a kind is not a migration.
- `EVENT_LOG_DATABASE` puts the log on another `DATABASES` alias, with
  `django_redis_aiogram.dbrouter.TelegramEventLogRouter` to move `migrate` with
  it. The writer names the alias explicitly, so the feature is correct with no
  router installed at all.
- Checks `E031`–`E042` and `W005`–`W009`, including the two that would otherwise
  only show up in production: an alias that is not in `DATABASES`, and a log
  switched on where the database has no engine. Both matter because the writer
  runs on a thread nobody is watching.
- **A read-only admin for the feed**, registered only when the flag is on and
  only when `django.contrib.admin` is installed. It is built for a table nobody
  wants to count: paging counts at most ten thousand rows inside a `LIMIT` and
  says when it stopped rather than reporting the cap as the answer, no date
  drilldown, exact-match search on the two indexed columns, and a filter built
  from the kind registry rather than from a `SELECT DISTINCT` over the table.
- Access splits in two: `view_telegramevent` for the list and the detail page,
  and `view_telegramevent_payload` for message bodies and exception text, so
  support can see that a message went out without reading what it said.
- `manage.py tgbot_prune_events` is what bounds the table. It deletes by
  primary-key range in bounded chunks, one transaction each, so it never holds
  a long lock and a range at the cold end cannot conflict with the inserts
  still arriving at the hot end. `--sleep` paces it for replicas, `--max-chunks`
  bounds a nightly run, `--dry-run` reports without deleting. With
  `EVENT_LOG_RETENTION_DAYS` unset it deletes nothing and says so — guessing a
  window would be a data-loss bug — and `W006` warns while it is unset. Two
  things the wiki page spells out and that bite after the fact: on PostgreSQL
  the space returns through autovacuum rather than immediately, so a large first
  prune wants a plain `VACUUM` afterwards (never `FULL`, which takes an
  exclusive lock); and attaching a `ForeignKey` to `TelegramEvent` disables
  Django's fast-delete path, so every prune then has to fetch primary keys
  first.
- **This package now ships a migration.** Run `manage.py migrate` after
  upgrading whether or not you turn the log on: the table is created either way,
  and creating it later on a live database is the more expensive order. It
  creates Django's four stock model permissions plus one,
  `view_telegramevent_payload`: there are no field-level permissions, so
  without it "saw that the message went out" and "read what it said" cannot be
  granted separately.

### Changed

- `ALLOW_PICKLE` is documented as what it is: the escape hatch for payloads JSON
  cannot describe, and not the 1.x upgrade window it was introduced as. Nothing
  about its behaviour changed — it is still off by default, the reader still
  refuses pickled payloads without it, and a refused payload is still left in
  flight rather than destroyed — on Redis 6.2 and newer. Without `LMOVE` there
  is no in-flight list, so a refused pickle is lost rather than held; the
  documentation now says so where it tells you the flag is safe to toggle. What changed is that the documentation no longer
  tells you to remove a setting you may need, and now says why the read path has
  a refusal branch at all.

### Documentation

- **Migrating from 1.x** is now **Upgrading**, covering each major release
  newest first. A 1.x to 3.0 jump has no shim to lean on, so it needed a page
  rather than a deleted one.

### Infrastructure

- The suite that needs no services is now two pytest invocations, alongside the
  Redis-backed one. `tests/settings.py` still configures no database — proving
  the package boots without one is part of what it tests — so database-backed
  tests live in `tests/db` under their own settings module and the default run
  ignores that directory. A new CI job runs it.

## 2.2.0 - 2026-08-04

### Changed

- **The redis floor is `>=6.2`**, up from `>=5.0`. The old floor promised
  support the package did not have: aiogram's `RedisStorage` calls `aclose()`,
  which redis-py added in 5.0.1, and aiogram's own extra asks for `>=6.2.0`. So
  `FSM_STORAGE: 'redis'` — the default — raised `AttributeError` on redis-py
  5.0.x, 6.0 and 6.1 while the metadata said those worked. Upgrading redis-py to
  6.2 or newer is the whole migration; `pip` does it on its own unless the
  version is pinned.

### Infrastructure

- The integration suite runs against a real Redis at **both ends** of the
  supported range, not only the newest. Running only the newest is what let a
  broken floor ship: `test-floors` installs the floors but the unit suite uses
  fakeredis, which has the `aclose()` redis-py 5.0 lacked, so the one
  combination that mattered — floors plus a real server — was never run.

## 2.1.1 - 2026-08-04

### Fixed

- `manage.py check` no longer warns about an untouched installation. 2.1.0
  shipped `REDIS_TIMEOUT` at 5 next to `BLPOP_TIMEOUT` at 5, so `W004` fired on
  every default configuration — and a warning nobody caused is what teaches
  people to stop reading the checks. The deadline defaults to 10, which also
  leaves the blocking pop at the 5 seconds it used before 2.1.0. A test now
  asserts the defaults report nothing at all.

## 2.1.0 - 2026-08-04

### Added

- `REDIS_TIMEOUT` (10 seconds) bounds how long any single Redis call may take,
  both connecting and waiting for an answer. Without it a server that accepts
  the connection and then stops responding holds the caller until the process
  is killed: redis-py only began applying a read deadline of its own in 8.0, and
  the supported floor is 5.0. Measured against a paused Redis 7 container —
  redis-py 5.0.0 never returned, 8.1.0 gave up after five seconds.
- Check `E030` for the new setting, and `W004` when `BLPOP_TIMEOUT` is at or
  above it.

### Fixed

- `BLPOP_TIMEOUT` is capped just below `REDIS_TIMEOUT`. A pop asked to wait
  longer than the socket will wait for an answer turns every idle round into a
  logged error — a consumer doing nothing wrong, complaining every few seconds
  — which is why the deadline could not simply be handed to the blocking read.
  The cap is reported by `W004` rather than applied silently.

## 2.0.1 - 2026-08-04

### Fixed

- The documentation links on the PyPI page. The README doubles as the long
  description, and PyPI serves it from `pypi.org` without rewriting links, so
  every `../../wiki/<page>` resolved to `pypi.org/wiki/<page>` — the whole
  documentation table, plus `LICENSE`, `CONTRIBUTING.md`, `AGENTS.md`,
  `CHANGELOG.md` and `SECURITY.md`, were dead there while working on GitHub.
  All of them are absolute now, and a test refuses any relative link in the
  README.
- `project.urls` declares `Documentation`, so the wiki appears in the PyPI
  sidebar rather than only inside the description.

## 2.0.0 - 2026-08-04

Upgrading the dependency needs no application-code changes: `telegram_bot`
still imports and still works in `INSTALLED_APPS`. Settings are a separate
matter — a 1.x queue needs `ALLOW_PICKLE` for the drain, and `parse_mode` moves
to `DEFAULT_BOT_PROPERTIES`. See the upgrade notes in the README.

### Breaking

- Package renamed to `django_redis_aiogram`. `telegram_bot` remains as a
  deprecated shim and is removed in 3.0.
- Requires Python 3.10–3.14, Django 5.2+, aiogram 3.30+. Django 4.2 reached
  end of life, and aiogram 3.30 needs Python 3.10.
- Queue payloads are serialized as JSON by default instead of pickle, and
  pickled payloads are **refused** by default — unpickling queue data is code
  execution. If the queue holds 1.x messages when you deploy, set
  `ALLOW_PICKLE: True` for the upgrade window and remove it once drained.
- Delivery defaults to `blpop` instead of keyspace expiry events. Set
  `DELIVERY: 'keyspace'` for the old behaviour.
- System check ids moved from `telegram_bot.EXXX` to `django_redis_aiogram.EXXX`.
- `TelegramBot` moved from `telegram_bot.telegram_bot` to
  `django_redis_aiogram.client`, and the settings module is
  `django_redis_aiogram.settings`. The package exports the `bot` and `conf`
  objects, which would otherwise shadow same-named submodules.
- `ENABLED` is parsed rather than coerced with `bool()`. The string `'false'`
  now disables the bot instead of enabling it, integers are accepted, and
  anything else raises `ImproperlyConfigured`.
- Packaging moved to `pyproject.toml` with a `src` layout.

### Added

- `ENABLED` lets a process opt out of the bot entirely: no autodiscover, no
  system checks, `send_raw` and `send_redis` become no-ops, and no credentials
  are required. Reads `TELEGRAM_BOT['ENABLED']` or the environment variable
  `DJANGO_REDIS_AIOGRAM_ENABLED`.
- Every scalar setting can come from `DJANGO_REDIS_AIOGRAM_<NAME>`; Django
  settings take precedence.
- `DEFAULT_BOT_PROPERTIES` maps onto aiogram's `DefaultBotProperties`, so
  `parse_mode` is configured once on the bot rather than injected into every
  call.
- `FSM_STORAGE` selects `redis` (default), `memory`, or a dotted path.
- `ALLOW_PICKLE` is the temporary opt-in for *reading* 1.x pickled payloads.
  Turn it off once the queue has drained; it is off by default.
- `AUTODISCOVER` can be turned off on its own.
- `start_tgbot --idle` keeps a disabled container parked instead of exiting,
  for restart policies that treat a clean exit as a crash loop.
- Public `bot.router`, `bot.dispatcher` and `bot.enabled`.
- `py.typed`: the package ships type information.
- `bot.send()` picks the route for you: direct inside the bot container,
  queued anywhere else.
- `RATE_LIMIT` paces outgoing calls under Telegram's published limits instead
  of waiting to be refused. Budgets are per bot, so a second token gets its own.
- `close()` releases the FSM storage as well as the bot session and the loop.
- `MODE` chooses where updates come from: `polling` (default) or `webhook`.
  Both are supported the same way; the choice can be made at startup through
  `DJANGO_REDIS_AIOGRAM_MODE`, or for one run with `start_tgbot --mode`.
- Webhook mode: `django_redis_aiogram.webhook.telegram_webhook` is a view you
  wire into your own `urls.py`, with `WEBHOOK_URL`, `WEBHOOK_SECRET` and
  `WEBHOOK_ALLOWED_UPDATES` to configure it, and `manage.py tgbot_webhook
  set|delete|info` to register it with Telegram. The secret is mandatory: the
  view refuses to serve without one and check `E027` says so before deployment.
- `manage.py tgbot_healthcheck` for container orchestration. The consumer
  publishes a heartbeat every `HEARTBEAT_INTERVAL` seconds with a TTL of three
  times that, so a dead consumer thread stops looking alive on its own; the
  command also fails when the queue grows past `HEALTHCHECK_MAX_QUEUE`.
- `django_redis_aiogram.enums` holds every value the settings accept —
  `DeliveryKind`, `SerializerKind`, `StorageKind`, `UpdateMode`,
  `SerializationTag`, `RateLimitKey` — so a project can import the enum instead
  of spelling a string. The values are frozen: queued payloads carry them.
- `django_redis_aiogram.exceptions` gives the package one error family.
  `DjangoRedisAiogramError` catches everything it raises;
  `SerializationError` and `UnknownApiMethodError` are the two a consumer is
  likely to name, and both keep their old import paths and base classes.
- Queued payloads may only name a Telegram API method aiogram exposes, and not
  `set_webhook`, `delete_webhook`, `log_out` or `close` — those administer the
  deployment rather than send. A payload naming anything else is refused when
  queued and dropped by the consumer, so whoever can write to Redis cannot
  reach `download_file` or the token.
- `import django_redis_aiogram` costs about a millisecond. Naming `bot` is what
  loads aiogram and the pydantic stack under it, and `ENABLED=0` keeps the
  package's own boot from naming it: no autodiscover, so no `tg_router` module
  is imported, and no checks are registered. A migration container or a CI run
  that imports nothing which sends never loads aiogram at all. The
  `telegram_bot` shim resolves its exports the same way, so a project still on
  the 1.x name pays for aiogram only where it sends.

### Fixed

- Importing the package no longer builds a bot, opens a Redis connection or
  creates an event loop. A missing token or Redis URL used to take the whole
  Django project down, including its test suite, in every process.
- System checks now actually validate. The old ones could not fail: the
  validation flag was only ever set inside an `isinstance` branch that a wrong
  type never entered.
- FSM state is no longer lost on restart — the dispatcher was built without a
  storage, so it defaulted to memory even with Redis configured.
- The delivery consumer no longer calls `create_task` on an event loop owned by
  another thread.
- Exhausting `MAX_RETRIES` now logs and honours `RAISE_EXCEPTION` instead of
  returning silently.
- Keyspace delivery reads the database index from `REDIS_URL` instead of
  assuming 0, and degrades to a warning when the server refuses `CONFIG SET`
  rather than crashing — managed Redis providers routinely refuse it.
- `send_redis` writes its expiry key with a real TTL; 1.x relied on positional
  arguments lining up.
- Keyspace delivery drains the queue with atomic pops. The 1.x lrange+ltrim
  pair let a second worker read the same messages and deliver them twice.
- Delivery is crash-safe on Redis 6.2+: a message is parked in a per-worker
  processing list while being sent and reclaimed on the next start, so a worker
  killed mid-send no longer loses it. After a crash a message may be sent
  twice. `WORKER_NAME` names that list when several workers share a host.
- The keyspace consumer no longer dies on `decode_responses` connections, and
  survives errors raised while handling a single event.
- Concurrent `send_raw` calls from a multi-threaded web server are serialized
  instead of failing with "this event loop is already running".
- Payloads are decoded correctly when `REDIS_URL` sets `decode_responses`.
- The `setting_changed` receivers use a `dispatch_uid`, so autoreload no longer
  stacks duplicates.
- Serializer failures surface as `SerializationError` instead of raw
  `TypeError` / `ValueError` escaping to the caller.
- Autodiscover no longer swallows `ImportError` raised inside a router module,
  so a broken router surfaces.
- Logging goes to the `django_redis_aiogram` logger instead of the root logger,
  with values in `extra` rather than interpolated into the message.
- `override_settings(TELEGRAM_BOT=...)` now takes effect; settings used to be
  frozen at import.
- SIGTERM shuts the worker down in order and closes the aiogram session, and
  restores the handler it replaced.
- A pickle payload the configuration refuses is no longer acknowledged and
  deleted. It stays in the in-flight list with a log line naming the cure, so a
  missed `ALLOW_PICKLE` during the upgrade window cannot silently destroy the
  1.x queue it was meant to drain.
- `ALLOW_PICKLE` is read the same way everywhere. From the environment it
  arrives as a string, and `'false'` used to be truthy — for the reader, for
  the writer and for check `E022`.
- `reclaim()` only gives up crash-safe delivery when the server truly lacks
  `LMOVE`; `WRONGTYPE` or a permission error no longer disables it for the life
  of the container, and a Redis that is unreachable at startup is retried
  instead of ending the consumer thread.
- The keyspace consumer builds its subscription inside its retry loop, drains
  the backlog at startup, and keeps its heartbeat fresh even when
  `BLPOP_TIMEOUT` is longer than the heartbeat interval.
- Shutdown refuses a send rather than losing it: a call arriving once `close()`
  has started is reported instead of being scheduled onto a loop that will
  never run it, and teardown holds the loop lock so it cannot interleave with a
  send driving the same loop.
- The shared Redis connection is built at most once and handed out atomically;
  a `reset()` racing a reader could previously return `None`.
- A rate limiter is no longer cached per bot, so `override_settings` and a
  changed `RATE_LIMIT` reach a bot that already exists.
- Per-chat rate-limit buckets are capped: eviction used to stop at the first
  bucket still owing wait time, so one busy chat kept the map growing.
- `manage.py check` survives settings a project got wrong in unusual ways — a
  non-string key in `TELEGRAM_BOT`, an unhashable member of
  `WEBHOOK_ALLOWED_UPDATES`, an unreadable `ALLOW_PICKLE` — reporting them
  rather than raising.

### Infrastructure

- Test suite covering lazy import, the `ENABLED` flag, serialization
  round-trips, delivery, checks and the shim.
- CI across Python 3.10–3.14 and Django 5.2/6.0 with ruff, mypy and pytest,
  plus a job pinning the lowest supported dependency versions. Every version the
  package advertises has to pass before a merge.
- Releases publish to PyPI through Trusted Publishing.
- Dependabot, issue and pull request templates, `CONTRIBUTING.md`,
  `SECURITY.md`.
- An integration suite that runs against a real Redis — `LMOVE` support, the
  reclaim path, keyspace notifications enabled at startup, a mixed pickle/JSON
  backlog, FSM state across a restart — plus `scripts/smoke_install.sh`, which
  installs the built wheel into a throwaway Django project and checks it boots
  with no credentials. Both run in CI.
- `ruff` runs with every rule enabled; deliberate exceptions carry their reason
  on the line. `mypy` covers the package in strict mode.
- Documentation lives in the wiki, published from `docs/wiki` on push to
  `master`, with the README kept to a front page. Configuration examples and
  testing recipes on those pages are executed by the test suite.
- `AGENTS.md` and the **AI assistants** wiki page: briefs for coding agents
  working on the package and with it.

## 1.0.0 - 2023-07-01
- Initial release

## 1.0.1 - 2023-07-01
- edit README.md

## 1.0.2 - 2023-07-01
- edit README.md

## 1.0.3 - 2023-07-01
- fix clearing messages from redis

## 1.0.4 - 2023-10-13
- update aiogram version
- change json to pickle, now supports more types of data

## 1.0.5 - 2023-10-19
- correcting README
- update settings to TypedDict
- add sending raw aiogram functions
- add possibility to send message from django

## 1.0.6 - 2023-10-21
- rm parse_mode by default
- add the ability to flexibly configure default kwargs for different aiogram functions
- edit min aiogram version

## 1.0.7 - 2023-10-21
- edit README

## 1.0.8 - 2024-02-20
- add max retries for sending message to settings (`MAX_RETRIES`)
- add reraise exception to `send_raw` (`RAISE_EXCEPTION`)