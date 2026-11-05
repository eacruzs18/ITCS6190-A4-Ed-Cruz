# Assignment #4: Report

**Name: Ed Cruz**
**Student ID: 801337361**
**Email: ecruz13@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801337361

The commands you ran, in order:
1. Generate the data
```bash
python3 datagen.py 801337361
```

2. Start the cluster
```bash
docker compose up -d
```

3. Run the script
```bash
docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py part1a /opt/spark/work-dir/shared <your student ID>
```
4. Check the output
```bash
ls shared-folder/output/part1a                               # one Parquet part per emitting batch
cat shared-folder/output/part1b/*.csv | grep -v ride_id | sort
cat shared-folder/output/part2/metrics/*.csv
cat shared-folder/output/part3/*.csv | grep -v ride_id | sort -t, -k9 -n -r | head
find shared-folder/models/fare -maxdepth 2
```
5. Stop the cluster
```bash
docker compose down
```

---

## Part 1a: trips and revenue per zone per window

The micro-batch summary (batches, watermark, state rows):

```
batch input rows            watermark state rows
    0         44                    -         27
    1         44  2026-10-27 11:57:52         30
    2         38  2026-10-27 11:58:48         31
    3         36  2026-10-27 11:59:47         31
    4         44  2026-10-27 12:00:51         25
    5         35  2026-10-27 12:01:49         42
    6         37  2026-10-27 12:02:52         46
    7         40  2026-10-27 12:03:58         49
    8         40  2026-10-27 12:04:49         50
    9         36  2026-10-27 12:05:53         25
   10         37  2026-10-27 12:06:50         38
   11         45  2026-10-27 12:07:48         47
   12         45  2026-10-27 12:08:52         49
   13         42  2026-10-27 12:09:57         50
   14         35  2026-10-27 12:10:46         25
   15         42  2026-10-27 12:11:52         43
   16         43  2026-10-27 12:12:48         49
   17         38  2026-10-27 12:13:47         49
   18         35  2026-10-27 12:14:55         50
   19         41  2026-10-27 12:15:49         25
   20         36  2026-10-27 12:16:53         38
   21         36  2026-10-27 12:17:52         45
   22         42  2026-10-27 12:18:54         48
   23         42  2026-10-27 12:19:53         49
   24         41  2026-10-27 12:20:54         24
   25         38  2026-10-27 12:21:56         40
   26         41  2026-10-27 12:22:58         46
   27         41  2026-10-27 12:23:58         48
   28         39  2026-10-27 12:24:49         49
   29         42  2026-10-27 12:25:56         25
   30          0  2026-10-27 12:26:42         25
```

Which 5-minute windows did the stream write, and which are missing compared with the batch
table printed after the query? For one window that was written, compare its trips with the
batch value: where did the difference come from (look at how `datagen.py` makes the late
events)? Why does `state rows` drop every five batches?

The stream wrote the windows up to 08:20, but not 08:25 to 08:30, because the watermark had not passed the end of that window when the stream stopped. In the 08:00 window for zone 2, the stream has 12 trips and the batch has 14. The two missing rides arrived about 8 minutes late, and Spark drops rides that arrive after their window has closed. The state rows drop every five batches because each batch is one minute of data, so a 5-minute window closes every five batches. When a window closes, Spark frees its rows, which lowers the count from about 50 to about 25.



## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

```

```

This query has no watermark and no state. Why is that correct here, and what would change if
you also wanted "more than three alerts for the same driver in ten minutes"?



---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the
"tuned" line with the best parameters, and the "best model" line:

```

```

Your tuned model: which estimator, which parameters in the grid and why those, how many fits
the CrossValidator ran, and how long part 2 took. Did the tuning beat the full linear
pipeline? If not, what does that tell you about the data?



Why is `hour` turned into a string before `StringIndexer` (see `features_of`) instead of
being used as a number?



---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation
and the per-window counts:

```

```

Run `python3 datagen.py <seed> shared-folder /tmp/truth.csv` (the third argument writes the
list of planted anomalies, which the generator does not reveal otherwise) and compare it with
your flagged rides: how many of the planted anomalies did you catch, and how many flagged
rides were not planted? Look at the false positives: what do they have in common, and what
would you change in part 2 to reduce them?



Why is there no `fit()` in part 3, and what would happen if you tried one on the stream?
Which stages of the loaded model are fitted models, and what did each one learn from the
training data?



---

## Design: this pipeline in production

One paragraph. The platform runs this for real: thousands of rides a minute from Kafka, a
model retrained every night on the trips of the last month, anomalies sent to a fraud team.
Where do the training job, the scoring job, the checkpoint and the model live, how does a
new model reach the running scoring job without losing events, and what would you monitor
to know that the model has gone stale?



---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

