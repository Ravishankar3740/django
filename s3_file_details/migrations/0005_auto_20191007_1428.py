# Generated by Django 2.2 on 2019-10-07 14:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('s3_file_details', '0004_mapping_action_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='filerecord',
            name='GSTIN',
            field=models.CharField(default=None, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name='filerecord',
            name='template_id',
            field=models.CharField(default=None, max_length=100, null=True),
        ),
    ]
