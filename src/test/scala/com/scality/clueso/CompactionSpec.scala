package com.scality.clueso.tools

import com.scality.clueso.compact.TableFilesCompactor
import com.scality.clueso.{PathUtils, SparkContextSetup, SparkUtils}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class CompactionSpec extends WordSpec with Matchers with SparkContextSetup {
  "Table Compactor" should {
    "Scenario 1: compact staging files with force" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactmebucket"
        val numberParquetFiles = "10"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "100", numberParquetFiles))

        // check landing files are created
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // compact
        compactor.compactLandingPartition("bucket", bucketName, 1, true)

        // then, landing should be empty
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // then staging should have new object
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
    }

    "Scenario 2: compact staging files without specifying bucket name" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactnoname"
        val numberParquetFiles = "10"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "100", numberParquetFiles))

        // check landing files were created correctly
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        //compact
        compactor.compact(1, true)

        // landing should be empty
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // should show staging obejct
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
    }

    "Scenario 3: compact staging files in multiple buckets without specifying bucket name" in withSparkContext {
      (spark, config) =>
        val bucketName1 = "compactnoname1"
        val bucketName2 = "compactnoname2"
        val numberParquetFiles = "10"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate both buckets
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName1, "100", numberParquetFiles))
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName2, "100", numberParquetFiles))

        // check landing files were created correctly
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName1/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName2/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // compact
        compactor.compact(1, true)

        // landing should  be empty
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName1/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 0
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName2/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 0

        // check staging files were created for both
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName1/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName2/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
    }

    "Scenario 4: compact staging files without having to force compaction" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactnoforce"
        val numberParquetFiles = "10"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "1000", numberParquetFiles))

        // check that landing files were created
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // compact
        compactor.compactLandingPartition("bucket", bucketName, 1, false)

        // check landing (should be empty again)
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // should now have staging object
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
    }
  }
}