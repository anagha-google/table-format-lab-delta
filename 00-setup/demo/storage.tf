/******************************************************
Create storage buckets
 ******************************************************/
resource "google_storage_bucket" "create_spark_bucket" {
  project                           = local.project_id 
  name                              = local.spark_bucket_nm
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
}

resource "google_storage_bucket" "create_data_bucket" {
  project                           = local.project_id 
  name                              = local.data_bucket_nm
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
}

resource "google_storage_bucket" "create_code_bucket" {
  project                           = local.project_id 
  name                              = local.code_bucket_nm
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
}

resource "google_storage_bucket" "create_sphs_bucket" {
  project                           = local.project_id 
  name                              = local.sphs_bucket_nm
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
}


/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/

resource "time_sleep" "sleep_after_bucket_creation" {
  create_duration = "60s"
  depends_on = [
    google_storage_bucket.create_spark_bucket,
    google_storage_bucket.create_data_bucket,
    google_storage_bucket.create_sphs_bucket,
    google_storage_bucket.create_code_bucket
  ]
}

/******************************************
Copy data to buckets
******************************************/

resource "google_storage_bucket_object" "create_dir_in_gcs" {
  for_each = fileset("./data/", "*")
  source = "./data/${each.value}"
  name = "${each.value}"
  bucket = "${local.data_bucket_nm}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

resource "google_storage_bucket_object" "upload_source_data_to_data_bucket" {
  for_each = fileset("./data/", "*")
  source = "./data/${each.value}"
  name = "parquet-source/${each.value}"
  bucket = "${local.data_bucket_nm}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation,
    google_storage_bucket_object.create_dir_in_gcs
  ]
}

/*******************************************
Customize managed notebook post-startup script
********************************************/

resource "null_resource" "create_mnbs_post_startup_bash" {
    provisioner "local-exec" {
        command = "cp ./templates/mnbs-exec-post-startup-template.sh ./code/mnbs-exec-post-startup.sh && sed -i  s/YOUR_PROJECT_NBR/${local.project_nbr}/g ./code/mnbs-exec-post-startup.sh"
    }
  depends_on = [
    google_storage_bucket.create_code_bucket
  ]
}

/******************************************
Copy code to the code bucket
******************************************/

resource "google_storage_bucket_object" "upload_code_to_code_bucket" {
  for_each = fileset("./code/", "*")
  source = "./code/${each.value}"
  name = "${each.value}"
  bucket = "${local.code_bucket_nm}"
  depends_on = [time_sleep.sleep_after_bucket_creation,
                null_resource.create_mnbs_post_startup_bash
  ]
}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/

resource "time_sleep" "sleep_after_bucket_uploads" {
  create_duration = "60s"
  depends_on = [
    google_storage_bucket_object.upload_source_data_to_data_bucket,
    google_storage_bucket_object.upload_code_to_code_bucket,

  ]
}
