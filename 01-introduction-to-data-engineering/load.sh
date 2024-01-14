#!/bin/bash

API_KEY='$2a$10$XrPmvZkaJ0sXYbP.vIfQaOi7QQ1hoip.5//mFNa696/Cs1R58L25q'
COLLECTION_ID='659a4d15266cfc3fde739a64'

curl -XPOST \
    -H "Content-type: application/json" \
    -H "X-Master-Key: $API_KEY" \
    -H "X-Collection-Id: $COLLECTION_ID" \
    -d @dogs.json \
    "https://api.jsonbin.io/v3/b"