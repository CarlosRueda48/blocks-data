SELECT
    id,
    shift_id,
    created_by_user_id,
    form_number,
    first_name,
    middle_name,
    last_name,
    name_suffix,
    voting_street_address_one,
    voting_street_address_two,
    voting_city,
    voting_state,
    voting_zipcode,
    mailing_street_address_one,
    mailing_street_address_two,
    mailing_city,
    mailing_zipcode,
    county,
    precinct,
    gender,
    date_of_birth,
    identification,
    phone_number,
    email_address,
    us_citizen,
    eligible_voting_age,
    signature,
    extras,
    created_at,
    updated_at,
    contacted_voter,
    score,
    delivery_id,
    attempted,
    party,
    name_prefix,
    ethnicity,
    latitude,
    longitude,
    distance_from_location,
    completed,
    uuid,
    van_id,
    ovr_status,
    online_registration,
    secondary_identification,
    metadata,
    person_id,
    smarty_streets_match_data,
    twilio_match_data
FROM
    onearizona.registration_forms