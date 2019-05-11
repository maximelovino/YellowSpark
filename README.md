# YellowSpark

_Authors: Maxime Lovino, Marco Rodrigues Lopes, David Wittwer_

YellowSpark is a project for a Big Data Analytics class at HES-SO Master. The project is based on NYC 2013 Taxi data that can be found [here](http://www.andresmh.com/nyctaxitrips/). The goal of the project is to compute analytics and train machine learning models on the taxi rides in the dataset. The project will be written in Scala and will use Spark to compute the analytics.

We will also use boundaries for the NYC boroughs available [here](https://nycdatastables.s3.amazonaws.com/2013-08-19T18:15:35.172Z/nyc-borough-boundaries-polygon.geojson) as GeoJSON data.

## Dataset description

The dataset consists of New York City taxi rides data from the year 2013. It is separated by month. For each month, two files are available: the `trip_data` and the `trip_fare`. The two files contains the exact same rides but not the same set of columns. There are duplicated columns between the files that will use to join them. An explanation of the story behind the retrieval of this dataset can be found [here](https://chriswhong.com/open-data/foil_nyc_taxi/).

The `trip_data` file has the following structure and contains the information about the taxi physical movement, time and distance:

```
root
 |-- medallion: string (nullable = true)
 |-- hack_license: string (nullable = true)
 |-- rate_code: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_time_in_secs: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- pickup_longitude: double (nullable = true)
 |-- pickup_latitude: double (nullable = true)
 |-- dropoff_longitude: double (nullable = true)
 |-- dropoff_latitude: double (nullable = true)
```

The `trip_fare` contains information about the cost and payment of the ride and has the following structure:

```
root
 |-- medallion: string (nullable = true)
 |-- hack_license: string (nullable = true)
 |-- vendor_id: string (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- surcharge: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- total_amount: double (nullable = true)
```

In order to join the two datasets, we use the `medallion`, which is the identifier of the taxi car, the `hack_license` which identifies the driver license and the `pickup_datetime`.

The joined dataset has the following structure:

```
root
 |-- medallion: string (nullable = true)
 |-- hack_license: string (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- rate_code: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_time_in_secs: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- pickup_longitude: double (nullable = true)
 |-- pickup_latitude: double (nullable = true)
 |-- dropoff_longitude: double (nullable = true)
 |-- dropoff_latitude: double (nullable = true)
 |-- vendor_id: string (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- surcharge: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- taxi_revenue: double (nullable = true)
```

At the moment, in order to avoid overloading our computers while still exploring and analysing the dataset, we have only worked on the January data and haven't opened the other file.

The January dataset contains 14'776'615 total rides before any cleaning or filtering, and weighs 2.46GB for the `trip_data` and 1.68GB for the `trip_fare`. We know that the files for the other months are more or less the same size or at least in the same order of magnitude so we should have more or less 150'000'000 total rides if we take into account the whole year.

## Features used and pre-processing

We added some calculated features to our dataset as well. First of all, we don't like ["retard units"](https://en.wikipedia.org/wiki/Imperial_units) so we added a `trip_distance_km` column to have the distance converted to kilometers. Then, we added the `average_speed_kmh` computed from `trip_time_in_secs` and `trip_distance_km`. The speed will used for one of our analysis as well as for filtering out bad data.

Then, we used the boroughs boundaries we had at our disposal and the pickup/dropoff longitude/latitude of the dataset to add `pickup_borough` and `dropoff_borough`. These two columns will also be used for filtering out bad data and one of our analysis.

Finally, for the fares part, we added `taxi_revenue` which is the sum of the fare and the tip for a ride and as such it computes the amount that goes in the driver's pocket for the ride.

### Data cleaning

First of all, we decided to drop any ride using the rate code #05, because as stated [here](https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page) and as seen in our dataset, the rides with rate code #05 are _Out of City Negotiated Flat Rate_ and in the dataset do not contain any information about duration, distance or locations, so we can't do anything with these rides.

Then, we noticed many "failed" rides with duration smaller or equal to 1 second, distance of 0.0 km, etc. We filtered out rides with duration smaller than 30 seconds, rides with distance smaller than 1km and finally we used the average speed we computed to filter out any ride with an average speed of more than 150 km/h (we could actually go further down with this limit).

There were also some failed coordinates for rides and we decided to remove them as well, basically we used the pickup and dropoff boroughs and filtered out all rides which had both started and ended outside of any borough boundaries.

## Analysis questions

- Trying to find a linear regression to estimate the fare amount of a ride (for each rate code) based on the distance and duration
- Analysis of the tips by pairs of boroughs (pickup-dropoff)
- Analysis of the average time to find the next ride based on the borough where the last ride finished (based on the given PDF)
- Building a machine learning to estimate traffic congestion based on the borough and time of day. The estimation of the congestion will be based on the average speed of the rides. We will evaluate our model by predicting speeds for test rides.
- Other global simple analysis:
  - Richest taxi driver
  - Most efficient taxi driver (most revenue/second ratio)
  - etc.

## Algorithms

## Optimisations

## Testing and evaluation

## Future improvements

