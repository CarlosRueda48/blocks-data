select 
    onearizona.shifts.id as shift_id,
	onearizona.shifts.shift_start,
	onearizona.shifts.shift_end,
	onearizona.shifts.field_start,
	onearizona.shifts.field_end,
    onearizona.shifts.soft_count_cards_total_collected,
    onearizona.shifts.soft_count_cards_complete_collected,
    onearizona.shifts.soft_count_cards_incomplete_collected,
    onearizona.locations.name location_name,
    onearizona.locations.street_address,
    onearizona.locations.city,
    onearizona.locations.county,
    onearizona.locations.latitude,
    onearizona.locations.longitude,
    onearizona.shifts.location_id,
    onearizona.canvassers.first_name canvasser_first_name,
    onearizona.canvassers.last_name canvasser_last_name,
    onearizona.shifts.canvasser_id,
    onearizona.turfs.name as turf_office_name,
    onearizona.canvassers.turf_id turf_offfice_id,
    onearizona.turfs.parent_id turf_parent_id,
    onearizona.turfs.turf_level_id
from onearizona.shifts
left join onearizona.locations on onearizona.shifts.location_id = onearizona.locations.id 
left join onearizona.canvassers on onearizona.shifts.canvasser_id = onearizona.canvassers.id
left join onearizona.turfs on onearizona.canvassers.turf_id = onearizona.turfs.id
where extract(year from field_start) = 2022
and extract(month from field_start) >= 01