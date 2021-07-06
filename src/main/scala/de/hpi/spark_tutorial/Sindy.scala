package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession


object Sindy {


  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    // Read files
    val data = inputs.map(inputFile => spark.read
      .option("delimiter", ";")
      .option("header", "true")
      .csv(inputFile))

    // Output (value, col) Tuples
    val valueColumnPairs = data.map(df => {
      val cols = df.columns
      df.flatMap(row => {
        for (i <- cols.indices) yield {
          (row.getString(i), cols(i))
        }
      })// <- dataset
    }) // <- list of datasets
    //combine results from files into a single rdd
    val flattenedPairs = valueColumnPairs.reduce((combined, newPair) => combined.union(newPair)).distinct()

    // Create Attribute Groups
    val attributeGroups = flattenedPairs
      .groupByKey(t => t._1)
      .mapGroups((_, iterator) => iterator.map(t => t._2).toSet)

    // Generate IND Candidates
    val INDCandidates = attributeGroups.flatMap(group => {
      group.map(col => (col, group - col))
    })

    // Generate all INDs
    val allINDs = INDCandidates
      .groupByKey(t => t._1)
      .mapGroups((key, iterator) => (key, iterator
        .map(t => t._2)
        .reduce((intersected, newCandidate) => intersected.intersect(newCandidate))))
      .filter(t => t._2.nonEmpty)

    // Output to Console
    val sorted = allINDs.sort("_1")

    def rowToString(row: (String, Set[String])): String = {
      row._1 + " < " + row._2.reduce((total, newString) => total + ", " + newString)
    }

    for (line <- sorted.collect) {
      println(rowToString(line))
    }

  }
}
