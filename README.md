##Spark Scheduler Simulator


###Background

The purpose of this project is to help understanding how the scheduling decisions are made by the task scheduling
policies in Apache Spark.

###Requirements

A copy of [Neptune Simulator Dependencies](https://github.com/pgaref/Neptune/tree/simulator_dependencies) is required in
the local Maven repository.

###Configuration

**ALL FIELDS IN THE CONFIGURATION ARE CURRENTLY REQUIRED**

The JSON configuration has the following structure:

```
{
  "simulatorConfiguration": {
    "executors": 4, // the number of executors to be simulated
    "coresPerExec": 1, // the number of cores per executor
    "sparkSchedulerMode": "FIFO" // the task scheduling policy (one of FIFO, FAIR or NEPTUNE)
    "sparkDefaultParallelism": 8, // the parallelism to be simulated
  },
  "jobs": [
    {
      "representation": {
        "stages": [
          {
            "id": 1, // the unique id of the stage
            "dependsOn": [] // the list of unique stage ids this stage depends on
          },
          {
            "id": 2,
            "dependsOn": [1]
          }
        ]
      },
      "properties": {
        "duration": 2, // the active task duration in simulator quantums
        "sparkSchedulerPool": "batch", // the pool this job will be assigned to if the FAIR scheduling policy is selected
        "neptunePri": 2 // the Neptune priority this job will have if the NEPTUNE scheduling policy is selected
      },
      "metaproperties": {
        "arrivesAt": 0, // defines at what quantum will a job of this type first arrive
        "repeatsEvery": 1, // defines how many quantums will pass before such a job arrives again 
        "repeatsTimes": 1 // defines how many times will a job of this type arrive
      }
    }
  ]
}
```