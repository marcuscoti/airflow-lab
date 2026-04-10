from airflow.models import BaseOperator
import json
import os
import shutil
from pathlib import Path
from airflow.sdk import get_current_context
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class ADLSUploadFileOperator(BaseOperator):
    template_fields = ("file","connection_id","source_file_path","container_name","blob_name")

    def __init__(
        self,
        *,
        file: str,
        connection_id: str,
        source_file_path: str,
        container_name: str,
        blob_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file = file
        self.connection_id = connection_id
        self.source_file_path = source_file_path
        self.container_name = container_name
        self.blob_name = blob_name

    def execute(self, context):
        hook = WasbHook(wasb_conn_id=self.connection_id)
        hook.load_file(
            file_path = f"{self.source_file_path}/{self.file}", 
            container_name = self.container_name, 
            blob_name = f"{self.blob_name}/{self.file}"
            )

    def expand(self, context):
        self.expand(context)



class ADLSListBlobsOperator(BaseOperator):
    template_fields = ("connection_id","container_name","prefix","endswith")

    def __init__(
        self,
        *,
        connection_id: str,
        container_name: str,
        prefix: str,
        endswith: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.container_name = container_name
        self.prefix = prefix
        self.endswith = endswith

    def execute(self, context):
        hook = WasbHook(wasb_conn_id=self.connection_id)
        ti = context['ti']
        processed = hook.get_blobs_list_recursive(container_name=self.container_name, prefix=self.prefix, endswith=self.endswith)
        print(f"Processed: {processed}")
        #ti.xcom_push(key='result', value=processed)
        return processed


class ADLSRemoveBlobOperator(BaseOperator):
    template_fields = ("connection_id","container_name","file")

    def __init__(
        self,
        *,
        connection_id: str,
        container_name: str,
        file: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.container_name = container_name
        self.file = file


    def execute(self, context):
        hook = WasbHook(wasb_conn_id=self.connection_id)
        hook.delete_file(container_name=self.container_name, blob_name=self.file, is_prefix=False, ignore_if_missing=True)


    def expand(self, context):
        self.expand(context)