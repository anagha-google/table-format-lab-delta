/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 

/** 
* Replace the text in angle brackets '<>' with the appropriate values for 
*  your argolis account and project
*/

variable "org_id" {
  type        = string
}

variable "location" {
  type        = string
}

variable "project_id" {
  type        = string
}

variable "project_nbr" {
  type        = string
}

variable "gcp_account_name" {
  type        = string
}

variable "biglake_location" {
  type        = string
  default     = "US"
}

variable "biglake_connection" {
  type        = string
  default     = "biglake-connection"
}

variable "bq_delta_ds" {
  type        = string
  default     = "delta_dataset"
}
