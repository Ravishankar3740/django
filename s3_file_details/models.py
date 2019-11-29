from rest_framework import status
from django.db import models
from django_mysql.models import JSONField
from jsonfield import JSONField
from django.core.files.storage import FileSystemStorage
from django.conf import settings
import os
import json
from django.shortcuts import HttpResponse
import os
from django.core.exceptions import ValidationError


class OverwriteStorage(FileSystemStorage):
  def get_available_name(self, name, max_length=None):
    if self.exists(name):
      # self.delete('media2/')
      os.remove(os.path.join(settings.MEDIA_ROOT,name))
    return name
fs = OverwriteStorage()

class OverwriteStorage2(FileSystemStorage):
  def get_available_name(self, name, max_length=None):
    if self.exists(name):
      # self.delete('media2/')
      os.remove(os.path.join(settings.MEDIA_ROOT2,name))
    return name
fs2 = OverwriteStorage()


class filerecord(models.Model):
    file = models.FileField(blank=False, null=False,storage=fs,default=None)
    filename=models.CharField(null=True,max_length=100,default=None)
    filetype=models.CharField(null=True,max_length=100,default=None)
    filename_Timestamp=models.CharField(blank=True,null=True,max_length=150,default=None)
    RawFileRef_Id=models.CharField(null=True,max_length=100,default=None)
    Action_Status=models.CharField(null=True,max_length=100,default=None)
    row_no=models.CharField(null=True,max_length=100,default=None)
    sheet_name=models.CharField(null=True,max_length=100,default=None)
    HQId=models.CharField(null=True,max_length=100,default=None)
    GSTIN=models.CharField(null=True,max_length=100,default=None)
    template_id=models.CharField(null=True,max_length=100,default=None)
    sub_state=models.CharField(null=True,max_length=100,default=None)
    stock_id=models.CharField(null=True,max_length=100,default=None)
    Branch_id=models.CharField(null=True,max_length=100,default=None)
    Timestamp=models.DateTimeField(auto_now_add=True,null=True)
    Uploaded_by=models.DateTimeField(auto_now_add=True,null=True)
    Uploadedtimestamp=models.DateTimeField(auto_now_add=True,null=True)
    Merge_by=models.CharField(null=True,max_length=100,default=None)
    Merge_Batch_Id=models.CharField(null=True,max_length=100,default=None)
    Deleted_by=models.CharField(null=True,max_length=100,default=None)
    Deleted_at=models.DateTimeField(auto_now_add=True,null=True)
    Archived_by=models.CharField(null=True,max_length=100,default=None)
    processed_by=models.CharField(null=True,max_length=100,default=None)
    raw_file_path=models.CharField(null=True,max_length=100,default=None)
    processed_file_path=models.CharField(null=True,max_length=100,default=None)
    error_file_path=models.CharField(null=True,max_length=100,default=None)
    control_summary_path=models.CharField(null=True,max_length=100,default=None)
    Merge_file_path=models.CharField(null=True,max_length=100,default=None)
    Company_Id = models.CharField(null=True,max_length=100,default=None)
    type=models.CharField(null=True,max_length=100,default=None)
    financial_year=models.CharField(null=True,max_length=100,default=None)
    month=models.CharField(null=True,max_length=100,default=None)
    month2=models.CharField(null=True,max_length=100,default=None)
    file_size=models.CharField(null=True,max_length=100,default=None)
    status=models.CharField(null=True,max_length=100,default=None)
    Merge_Reference=models.CharField(blank=True,null=True,max_length=100,default=None)
    Row_Size = models.CharField(blank=True, null=True, max_length=100, default=None)
    Column_Size = models.CharField(blank=True, null=True, max_length=100, default=None)
    Unique_Invoices = models.CharField(blank=True, null=True, max_length=100, default=None)
    Total_Tax_Value = models.CharField(blank=True, null=True, max_length=100, default=None)
    Total_Taxable_Value = models.CharField(blank=True, null=True, max_length=100, default=None)
    Reason=models.TextField(default=None)


class Mapping(models.Model):
    Template_Name = models.CharField(null=True, max_length=100, default=None)
    column_header = JSONField(blank=True, null=True)
    filename = models.CharField(null=True, max_length=100, default=None)
    row_no = models.CharField(null=True, max_length=100, default=None)
    sheet_name = models.CharField(null=True, max_length=100, default=None)
    Company_Id = models.CharField(null=True, max_length=100, default=None)
    filename_Timestamp=models.CharField(blank=True,null=True,max_length=150,default=None)
    Type = models.CharField(null=True, max_length=100, default=None)
    Action_Status=models.CharField(null=True,max_length=100,default=None)
    on_off = models.BooleanField(null=True, max_length=100, default=None)

class Mastertable(models.Model):
    file = models.FileField(blank=False, null=False,storage=fs2,default=None)
    sheet_name = models.CharField(null=True, max_length=100, default=None)
    row_no = models.CharField(null=True, max_length=100, default=None)
    Company_Id = models.CharField(null=True, max_length=100, default=None)
    Type = models.CharField(null=True, max_length=100, default=None)
    file_size=models.CharField(null=True,max_length=100,default=None)
    Uploadedtimestamp=models.DateTimeField(auto_now_add=True,null=True)
    Uploaded_by=models.DateTimeField(auto_now_add=True,null=True)
    filename_Timestamp=models.CharField(blank=True,null=True,max_length=150,default=None)
    MasterFileColumns = models.TextField(default='Na')


class TgComparisionandarithmeticandfuction2(models.Model):
    data_in = models.CharField(db_column='Data_in', max_length=20, blank=True, null=True)  # Field name made lowercase.
    new_column = models.CharField(db_column='New_column', max_length=50, blank=True, null=True)  # Field name made lowercase.
    rule_id = models.IntegerField(db_column='Rule_id', blank=True, null=True)  # Field name made lowercase.
    column_value = models.CharField(db_column='Column_value', max_length=50, blank=True, null=True)  # Field name made lowercase.
    header_col1 = models.CharField(db_column='Header_Col1', max_length=200, blank=True, null=True)  # Field name made lowercase.
    oprator1 = models.CharField(db_column='Oprator1', max_length=50, blank=True, null=True)  # Field name made lowercase.
    header_col2 = models.CharField(db_column='Header_Col2', max_length=200, blank=True, null=True)  # Field name made lowercase.
    oprator2 = models.CharField(db_column='Oprator2', max_length=50, blank=True, null=True)  # Field name made lowercase.
    values = models.CharField(db_column='Values', max_length=50, blank=True, null=True)  # Field name made lowercase.
    connector = models.CharField(max_length=50, blank=True, null=True)
    set_value = models.CharField(db_column='Set_value', max_length=50, blank=True, null=True)  # Field name made lowercase.
    status = models.CharField(db_column='Status', max_length=50, blank=True, null=True)  # Field name made lowercase.
    company_name = models.CharField(db_column='Company_name', max_length=200, blank=True, null=True)  # Field name made lowercase.
    companyid = models.IntegerField(db_column='companyId', blank=True, null=True)  # Field name made lowercase.
    template_name = models.CharField(db_column='Template_Name', max_length=100, blank=True, null=True)  # Field name made lowercase.
    template = models.ForeignKey(Mapping, models.DO_NOTHING, db_column='Template_id', blank=True, null=True)  # Field name made lowercase.
    master_id = models.CharField(db_column='Master_id', max_length=10, blank=True, null=True)  # Field name made lowercase.
    ruletype = models.CharField(db_column='ruleType', max_length=20, blank=True, null=True)  # Field name made lowercase.
    type = models.CharField(db_column='Type', max_length=35, blank=True, null=True)  # Field name made lowercase.

















