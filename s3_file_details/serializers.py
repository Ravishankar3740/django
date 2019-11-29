from rest_framework import serializers
from .models import *

class UploadSerializer(serializers.ModelSerializer):
  class Meta():
    model = filerecord
    fields = '__all__'


class Mapping_serializer(serializers.ModelSerializer):
  class Meta():
    model = Mapping
    fields = '__all__'

class Master_table_serializer(serializers.ModelSerializer):
  class Meta():
    model = Mastertable
    fields = '__all__'
