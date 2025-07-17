SELECT 
  (SELECT COUNT(*) FROM properties) AS property_count,
  (SELECT COUNT(*) FROM valuations) AS valuation_count,
  (SELECT COUNT(*) FROM hoa_fees) AS hoa_count,
  (SELECT COUNT(*) FROM rehab_estimates) AS rehab_count;

-- No valuation
SELECT id, address FROM properties
WHERE id NOT IN (SELECT DISTINCT property_id FROM valuations);

-- No HOA
SELECT id, address FROM properties
WHERE id NOT IN (SELECT DISTINCT property_id FROM hoa_fees);

-- No rehab
SELECT id, address FROM properties
WHERE id NOT IN (SELECT DISTINCT property_id FROM rehab_estimates);

SELECT * FROM valuations v
WHERE NOT EXISTS (
  SELECT 1 FROM properties p WHERE p.id = v.property_id
);

SELECT * FROM hoa_fees h
WHERE NOT EXISTS (
  SELECT 1 FROM properties p WHERE p.id = h.property_id
);

SELECT * FROM rehab_estimates r
WHERE NOT EXISTS (
  SELECT 1 FROM properties p WHERE p.id = r.property_id
);
-- Basic join
SELECT 
  p.id, p.address, p.city, p.state, 
  v.list_price, v.expected_rent,
  h.hoa, r.rehab_calculation
FROM properties p
LEFT JOIN valuations v ON p.id = v.property_id
LEFT JOIN hoa_fees h ON p.id = h.property_id
LEFT JOIN rehab_estimates r ON p.id = r.property_id
LIMIT 10;


