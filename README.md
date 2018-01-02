# Ephemeral Kafka Producer Test

The purpose of this app is to test the effects of frequently creating and 
destroying kafka producers on the health of the Kafka cluster.

**Warning:** While testing against Kafka 1.0.0 cluster this tool effectively 
renders a cluster unusable.

## What does it do?

This tool spins up a pool of goroutines that (in a tight loop):
* Create a Kafka producer
* Writes to a topic
* Destroys the producer

The `CONCURRENCY` environment variable dictates the size of the pool.

## Usage

* Create Kafka addon: `heroku addons:create heroku-kafka:standard-0 --robot`
* Deploy
* Tweak: `heroku config:set CONCURRENCY=X`
* Observe!

