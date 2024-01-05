# MapReduce Project: Top 5 Most Played Songs

## Introduction

This code illustrates the MapReduce concept by analyzing 15 log files containing song names. The objective is to count the 5 most played songs based on user interactions, where each "play song" click generates a log record.

The architecture simplifies MapReduce details, requiring users only to provide a delegate function. This reduces the interaction overhead with Map, Shuffle, and Reduce classes.

## Directory Overview

- **Delegate**: Contains delegate function definitions for Map, Shuffle, and Reduce.
- **FileSystemService**: Manages interactions with the machine's file system.
- **Logger**: A basic logger for debugging or understanding code execution flow.
- **Logs**: 15 log files recording played songs.
- **Mapper**: The Mapper class.
- **MapReduceManager**: Executes the MapReduce concept in parallel.
- **Reducer**: The Reducer class.
- **Shuffler**: The Shuffler class.

## Project Objective

The goal is to count the 5 most played songs from the log files located in the Logs directory. Each log file contains a list of played songs. The task involves counting the occurrences of each song and returning the top 5 most played songs.

## Mapper Class

Responsible for reading log files in parallel, creating a list of song names with occurrence counts (each song treated as unique, even if duplicated), and handling data writing to the file system.

## Shuffler Class

Aggregates data from the Mapper, combining song names, and writes the result to the file system.

## Reducer Class

Reads aggregated data from the file system, counts occurrences of each song, and handles data writing to the file system.

## Combiner Function

Aggregates data from Reducers to produce the final result: a list of key-value pairs showing song names and counts. This list is then sorted, and the top 5 most played songs are displayed on the console.
