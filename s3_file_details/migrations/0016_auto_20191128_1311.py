# Generated by Django 2.2.4 on 2019-11-28 13:11

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('s3_file_details', '0015_mastertable_masterfilecolumns'),
    ]

    operations = [
        migrations.RenameField(
            model_name='filerecord',
            old_name='return_type',
            new_name='RawFileRef_Id',
        ),
    ]
