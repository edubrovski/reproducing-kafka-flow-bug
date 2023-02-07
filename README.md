# How to reproduce the bug:

1. Build the project and run `Test`. I personally run it in Intellij with these VM options:
```
-XX:ActiveProcessorCount=2 -Dcats.effect.tracing.mode=full -Dcats.effect.tracing.buffer.size=32
```
Our fake Kafka consumer is expected to poll infinitely.
It should print a "done" message to stdout after every poll. Example:
```
2023-02-07T09:34:03.099534Z - done
```

2. Now just wait. Sometimes the code will hang in 5 minutes, sometimes it'll run fine for hours. It's hard to reproduce.
The best strategy is to restart the code every 30 minutes until it hangs.

3. You'll know that it hanged because you'll see something like this in stdout:
```
Currently keys in memory: 65000
Currently keys in memory: 65000
Currently keys in memory: 65000
Currently keys in memory: 65000
Currently keys in memory: 65000
Currently keys in memory: 65000
Currently keys in memory: 65000
Currently keys in memory: 65000
```

This means that the consumer stopped polling because it's not printing "done".
At this moment you can take a fiber dump, use Intellij's profiler, etc.

4. You can confirm that CE2 version doesn't hang by switching to `ce2` branch and running the program multiple times.

# Implementation details

To reproduce this we used [kafka-flow](https://github.com/evolution-gaming/kafka-flow).
We replaced `kafka-flow-metrics` and `scache` with something very simple to make the code easier to understand.

