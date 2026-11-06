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

The stream wrote the windows up to 08:20, but not 08:25 because the final watermark never passed 8:30. In the 08:00 window for zone 2, the stream has 12 trips and the batch has 14. The two missing rides arrived about 8 minutes late, and Spark drops rides that arrive after their window has closed. The state rows drop every five batches because each batch is one minute of data, so a 5-minute window closes every five batches. When a window closes, Spark frees its rows, which lowers the count from about 50 to about 25.



## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

The query found 7 alerts, all of the kind `zero_distance`. There were no `high_fare` alerts, so no ride had a fare above 150.

```
=== part1b: alert rows ===
+-------+-------------------+-----------+-----------+------------+-----+-------------+
|ride_id|event_time         |pickup_zone|distance_km|duration_min|fare |reason       |
+-------+-------------------+-----------+-----------+------------+-----+-------------+
|R00026 |2026-10-27 08:00:19|18         |0.0        |13.5        |16.2 |zero_distance|
|R00166 |2026-10-27 08:03:09|16         |0.0        |25.0        |31.38|zero_distance|
|R00649 |2026-10-27 08:16:06|1          |0.0        |27.6        |39.86|zero_distance|
|R00739 |2026-10-27 08:18:48|3          |0.0        |14.2        |15.05|zero_distance|
|R00931 |2026-10-27 08:22:51|17         |0.0        |15.6        |16.04|zero_distance|
```

This query has no watermark and no state. Why is that correct here, and what would change if
you also wanted "more than three alerts for the same driver in ten minutes"?

This query needs no state or watermark because each ride is checked on its own. A watermark only decides when to close 
a window and free memory, and this query has no windows.

To get more than three alerts for the same driver in ten minutes, Spark would have to keep a running count for each 
driver, which is state, and use a watermark to know when to drop old counts. 
---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the
"tuned" line with the best parameters, and the "best model" line:

```
baseline   RMSE   3.67   R2  0.854
full       RMSE   1.67   R2  0.970
tuned      RMSE   2.54   R2  0.930
tuned      best numTrees 50, maxDepth 8; 13 fits
best model: full
```

Your tuned model: which estimator, which parameters in the grid and why those, how many fits
the CrossValidator ran, and how long part 2 took. Did the tuning beat the full linear
pipeline? If not, what does that tell you about the data?

The tuned model is a `RandomForestRegressor` on the index columns plus distance and duration. The grid has `numTrees` in [20, 50] and `maxDepth` in [5, 8]. More trees make the forest more stable, and a deeper tree can fit more detail but may overfit. The CrossValidator ran 13 fits. Part 2 took about 35 seconds on my laptop. The tuning did not beat the full pipeline: RMSE 2.54 against 1.67. This tells us the fare is almost a straight-line function of distance and duration. A linear model fits that well, while trees predict in steps and cannot follow a straight line as closely.

Why is `hour` turned into a string before `StringIndexer` (see `features_of`) instead of
being used as a number?

The hour is a category, not an amount. As a number, the model would assume the fare goes up or down by the same step for every hour, and that hour 23 is far from hour 0. As a string, each hour gets its own one-hot column and its own weight.


---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation
and the per-window counts:

```
saved model: full, test RMSE 1.67; flagging rides with deviation > 6.68

=== part3: 68 rides flagged ===
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|ride_id|event_time         |pickup_zone|distance_km|duration_min|payment_type|fare  |prediction|deviation|
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|R00232 |2026-10-27 08:05:16|7          |15.31      |55.3        |card        |136.9 |51.56     |85.34    |
|R00721 |2026-10-27 08:17:12|7          |9.25       |30.1        |card        |107.97|32.26     |75.71    |
|R00199 |2026-10-27 08:04:31|5          |4.21       |17.8        |card        |83.41 |28.4      |55.01    |
|R00768 |2026-10-27 08:19:53|3          |6.19       |25.4        |card        |76.8  |25.39     |51.41    |
|R00386 |2026-10-27 08:08:59|20         |7.8        |28.1        |cash        |72.6  |30.07     |42.53    |
|R00856 |2026-10-27 08:20:56|5          |3.99       |14.4        |card        |66.21 |26.78     |39.43    |
|R00983 |2026-10-27 08:22:16|5          |5.57       |19.6        |card        |70.66 |31.3      |39.36    |
|R00913 |2026-10-27 08:23:24|18         |6.3        |19.0        |card        |59.49 |23.37     |36.12    |
|R00806 |2026-10-27 08:19:58|1          |4.73       |14.7        |card        |56.89 |21.61     |35.28    |
|R01157 |2026-10-27 08:28:54|7          |4.31       |14.2        |card        |52.48 |18.25     |34.23    |
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+

=== part3: flagged rides per 5-minute window ===
+-------------------+-----+
|       window_start|count|
+-------------------+-----+
|2026-10-27 08:00:00|    9|
|2026-10-27 08:05:00|   15|
|2026-10-27 08:10:00|    9|
|2026-10-27 08:15:00|   16|
|2026-10-27 08:20:00|    8|
|2026-10-27 08:25:00|   11|
+-------------------+-----+
```

Run `python3 datagen.py <seed> shared-folder /tmp/truth.csv` (the third argument writes the
list of planted anomalies, which the generator does not reveal otherwise) and compare it with
your flagged rides: how many of the planted anomalies did you catch, and how many flagged
rides were not planted? Look at the false positives: what do they have in common, and what
would you change in part 2 to reduce them?

The threshold is 6.68, and 68 rides were flagged. The generator planted 52 anomalies (30 too high, 22 too low). All 52 were caught, and 16 flagged rides were not planted.

The false positives have two things in common. 12 are long, expensive trips that the model predicted a little too low, by 6.8 to 19.4, just above the threshold. The other 4 have a distance of zero. In part 2, I would train on the log of the fare, so the error is measured in percent and long trips are not punished. 

Why is there no `fit()` in part 3, and what would happen if you tried one on the stream?
Which stages of the loaded model are fitted models, and what did each one learn from the
training data?

The model is already trained and saved, so part 3 only applies it. A `fit()` needs to read all the data more than once, and a stream never ends, so Spark does not allow it. Scoring should also never change the model while it runs.

The fitted stages are:
- **3 StringIndexers (zone, payment type, hour):** the list of values of each column, and the number given to each value.
- **OneHotEncoder:** how many categories each column has.
- **LinearRegression:** one weight per feature, plus an intercept.

The VectorAssembler is not fitted. It only puts the columns together.


---

## Design: this pipeline in production

One paragraph. The platform runs this for real: thousands of rides a minute from Kafka, a
model retrained every night on the trips of the last month, anomalies sent to a fraud team.
Where do the training job, the scoring job, the checkpoint and the model live, how does a
new model reach the running scoring job without losing events, and what would you monitor
to know that the model has gone stale?

The nightly training job is a batch Spark job that reads from shared storage, such as a data lake. 
It saves each new model in its own dated folder, for example models/fare/2026-11-06. The scoring job is a separate 
streaming job that reads rides from Kafka, flags the odd ones, and sends them to the fraud team. Its checkpoint lives 
on shared storage. The checkpoint remembers the last Kafka offset it processed. When a new model is ready, the scoring 
job is restarted to load it, and it continues from the saved offset, so no rides are lost. To know when the model has 
gone stale, I would watch the share of rides flagged per window, since a sudden jump or drop is a warning.



---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.
Nothing went wrong.

