# Generated by Django 2.2.4 on 2019-11-29 14:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('s3_file_details', '0016_auto_20191128_1311'),
    ]

    operations = [
        migrations.AddField(
            model_name='filerecord',
            name='Column_Size',
            field=models.CharField(blank=True, default=None, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name='filerecord',
            name='Row_Size',
            field=models.CharField(blank=True, default=None, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name='filerecord',
            name='Total_Tax_Value',
            field=models.CharField(blank=True, default=None, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name='filerecord',
            name='Total_Taxable_Vale',
            field=models.CharField(blank=True, default=None, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name='filerecord',
            name='Unique_Invoices',
            field=models.CharField(blank=True, default=None, max_length=100, null=True),
        ),
    ]