import pytest
import requests

from requests.exceptions import ConnectionError


def test_basketball_reference_responsive(bbref_url):
   response = requests.get(bbref_url)
   assert response.status_code == 200

def test_draftkings_responsive(draftkings_url):
   response = requests.get(draftkings_url)
   assert response.status_code == 200


def test_divisible_by_3(input_value):
   assert input_value % 5 == 0