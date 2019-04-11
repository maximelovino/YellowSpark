# YellowSpark

YellowSpark is a project for a Big Data Analytics class at HES-SO Master. The project is based on NYC 2013 Taxi data that can be found [here](http://www.andresmh.com/nyctaxitrips/). The goal of the project is to compute analytics on the taxi rides in the dataset. The project will be written in Scala and will use Spark to compute the analytics.

## Analytics computed

The full set of analytics we will compute isn't fixed yet but the goal is to include:

- Taxi driver earning stats (average, total) using the `fares` file also present on the site
- Distance stats (average, total, etc) for the rides
- Occupation percentage for taxis
- Statistics by borough
- Most used pickup areas
- Most used dropoff areas
- Most profitable areas
- Most profitable time of the day
- Average speed related to time of the day and areas (estimation for traffic congestion)