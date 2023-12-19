SELECT * FROM diffusion_shared.nem_scenario_bau_2019
where state_abbr='WA';


--For NEM scenario
UPDATE diffusion_shared.nem_scenario_bau_2019
SET sunset_year = 3000
WHERE state_abbr = 'WA';

--For NEM sunset scenario
UPDATE diffusion_shared.nem_scenario_bau_2019
SET sunset_year = 2029
WHERE state_abbr = 'WA';



SELECT * FROM diffusion_shared.nem_state_limits_2019
where state_abbr='WA';
                
--For NEM scenario
UPDATE diffusion_shared.nem_state_limits_2019
SET sunset_year = 3000
WHERE state_abbr = 'WA';


--For NEM sunset scenario
UPDATE diffusion_shared.nem_state_limits_2019
SET sunset_year = 2029
WHERE state_abbr = 'WA';


SELECT * FROM diffusion_shared.nem_scenario_bau_by_utility_2019
where eia_id='20169';


--For NEM scenario
UPDATE diffusion_shared.nem_scenario_bau_by_utility_2019
SET sunset_year = 3000
where eia_id='20169';


--For NEM sunset scenario
UPDATE diffusion_shared.nem_scenario_bau_by_utility_2019
SET sunset_year = 2029
where eia_id='20169';
