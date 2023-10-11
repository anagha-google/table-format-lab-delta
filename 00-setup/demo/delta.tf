/******************************************************
Create BQ Connection
 ******************************************************/
resource "google_bigquery_connection" "biglake_connection" {
   connection_id = "${var.biglake_connection}"
   location      = "${var.biglake_location}"
   friendly_name = "${var.biglake_connection}"
   description   = "Connection to demonstrate connecting Delta Tables to BigLake"
   cloud_resource {}   
   depends_on = [
    time_sleep.sleep_after_iam_permissions_grants
  ]  

}


/******************************************************
Give BQ Connection service account storage privs
 ******************************************************/
resource "null_resource" "create_groups" {
  provisioner "local-exec" {
    command = <<-EOT
        export BIGLAKE_SA=$(bq show --connection --format json "${var.project_id}.${var.biglake_location}.${var.biglake_connection}" \
            | jq -r .cloudResource.serviceAccountId)

        gsutil iam ch serviceAccount:$BIGLAKE_SA:objectViewer gs://${local.data_bucket_nm}
    EOT
  }
  depends_on = [
    google_bigquery_connection.biglake_connection
  ]
}

/**********************************************************
Create BQ Dataset for later adding delta table via 
manifest files
 *********************************************************/
resource "google_bigquery_dataset" "delta_dataset_creation" {
  dataset_id                  = var.bq_delta_ds
  location                    = "${var.biglake_location}"
  depends_on = [
    google_bigquery_connection.biglake_connection
  ]
}
