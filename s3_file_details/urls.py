
from django.urls import path, include
from .views import *
from .uploadEngine import *

urlpatterns = [
    path('upload/', UploadView2.as_view()),
    path('sheet_viewrule/', Sheet_ViewRule.as_view()),
    path('test/', testt.as_view()),
    path('mast_file_read/', Masterfile_columns.as_view()),
    path('get_masterfiles/', Get_MaaterFile_list.as_view()),
    path('master_file/', Master_table.as_view()),
    path('processed/', Processed.as_view()),
    path('del_temp/', Delete_temp.as_view()),
    path('temp_eye/', Mappingview.as_view()),
    path('deleteview/', DeleteView.as_view()),
    path('start/', StartView.as_view()),
    path('filter/', Filter.as_view()),
    path('delete/', Delete_files.as_view()),
    path('query/', hard_query.as_view()),
    path('sys_col/', system_col.as_view()),
    path('sheet_read/', Mapping_file_sheets.as_view()),
    path('sheet_row_temp/', file_details_sheet_row.as_view()),
    path('col_json/', Col_header.as_view()),
    path('select_template/',choose_template.as_view()),
    path('tg_process/',Tg_File_Processing.as_view()),
    path('client_process/',client_file_processing.as_view()),
    path('file_download/',download_file.as_view()),
    path('create_rule/',Create_Rule.as_view()),
    path('upload_engine/',upload_engine.as_view()),
    path('download_rule/',Download_RuleBuilder_Rule.as_view()),
]
