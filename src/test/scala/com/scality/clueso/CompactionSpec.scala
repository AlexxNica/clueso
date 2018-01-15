package com.scality.clueso.tools

import com.scality.clueso.compact.TableFilesCompactor
import com.scality.clueso.{PathUtils, SparkContextSetup, SparkUtils}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class CompactionSpec extends WordSpec with Matchers with SparkContextSetup {
  "Table Compactor" should {
    "Scenario 1: compact staging files" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactmebucket"
        val numberParquetFiles = "10"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "100", numberParquetFiles))
        val fs = SparkUtils.buildHadoopFs(config)
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // compact
        val compactor = new TableFilesCompactor(spark, config)
        compactor.compactLandingPartition("bucket", bucketName, 1, true)

        // then, landing should be empty
        fs.listStatus(new Path(PathUtils.landingURI, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // then staging should have...


        // do tests without force true and without specifying a bucket name
        
    }
  }
}