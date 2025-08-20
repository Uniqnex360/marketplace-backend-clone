import pytest
from django.urls import reverse
from rest_framework.test import APIClient
def test_signupuser():
    client=APIClient()
    url=reverse('signupUser')
    data={
        "email":"test@example.com",
        'password':'password123'
    }
    response=client.post(url,data,format='json')
    assert response.status_code==200
    assert 'success' in response.data.get('message',"").lower()