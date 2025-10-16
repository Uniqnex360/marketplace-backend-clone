from omnisight.models import Marketplace
from bson import ObjectId


def get_filtered_marketplaces(countries=None, marketplace_id=None):
    """
    Filter marketplaces by country or marketplace_id
    Args:
        countries: Can be a string ('US', 'UK') or list of strings (['US', 'UK'])
        marketplace_id: Specific marketplace ID or 'all'
    """

    if marketplace_id and marketplace_id != 'all':
        try:
            return [ObjectId(marketplace_id)] if isinstance(marketplace_id, str) else [marketplace_id]
        except Exception:
            raise ValueError("Invalid marketplace_id format")

    if countries:

        if isinstance(countries, str):
            countries = [countries]

        if isinstance(countries, list) and len(countries) > 0:
            country = countries[0].upper()
            if country not in ['US', 'UK']:
                raise ValueError("Country must be US or UK")

            marketplaces = Marketplace.objects.filter(country__in=[country])
            return [mp.id for mp in marketplaces]

    marketplaces = Marketplace.objects.filter(country__in=["US"])
    return [mp.id for mp in marketplaces]
