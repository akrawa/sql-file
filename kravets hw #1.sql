--select ad_date, spend, clicks, spend /clicks as sp_to_cl
--from facebook_ads_basic_daily fabd
--where clicks > 0
--order by ad_date desc;

/**

select ad_date, campaign_id,
sum(spend) as tot_spend,
sum(impressions) as tot_impr,
sum(clicks) as tot_clicks,
sum(value) as tot_values,
sum(spend)/sum(clicks) as cpc,
1000 * sum(spend)/ sum(impressions) as cpm,
cast (sum(clicks) as numeric)/ sum (impressions) as ctr,
cast (sum(value) as numeric)/ sum(spend) as romi
from facebook_ads_basic_daily fabd
where spend >0 and impressions >0 and clicks >0
group by ad_date, campaign_id;

**/
/**
select campaign_id, sum(spend) as tot_spend, 
sum(value) ::numeric / sum(spend) as romi 
from facebook_ads_basic_daily fabd
group by campaign_id
having sum(spend) > 500000
order by romi desc 
limit 1;

**/
/**
with fb_and_google as (
select ad_date,
'Facebook_ads' :: text as media_cource,
spend, impressions, reach, clicks, leads, value
from facebook_ads_basic_daily fabd
union
select ad_date,
'Google_ads' :: text as media_cource,
spend, impressions, reach, clicks, leads, value
from google_ads_basic_daily gabd)
select ad_date,media_cource,
sum(spend) as tot_spend,
sum(impressions) as tot_impr,
sum(clicks) as tot_clicks,
sum(value) as tot_values
from fb_and_google
where spend >0 and impressions >0 and clicks >0
group by ad_date, media_cource;
**/
/**

with fb_all as (
select fabd.ad_date, fabd.impressions, fabd.reach, fabd.clicks, 
fabd.leads, fabd.value, fabd.spend, fc.campaign_name, fa.adset_name,
'Facebook_ads' :: text as media_source
from facebook_ads_basic_daily fabd 
right join facebook_campaign fc 
on fc.campaign_id = fabd.campaign_id 
right join facebook_adset fa 
on fa.adset_id = fabd.adset_id),
fb_and_gl_all as(
select ad_date, impressions, reach, clicks, leads, value, spend, 
campaign_name, adset_name, media_source
from fb_all
union
select gabd.ad_date, gabd.impressions, gabd.reach, gabd.clicks,
gabd.leads, gabd.value, gabd.spend, 
gabd.campaign_name, gabd.adset_name,
'Google_ads' :: text as media_source
from google_ads_basic_daily gabd)
select ad_date, media_source, campaign_name, adset_name,
sum(spend) as tot_spend,
sum(impressions) as tot_impr,
sum(clicks) as tot_clicks,
sum(value) as tot_values
from fb_and_gl_all
group by ad_date, media_source, campaign_name, adset_name;

/**
**/
with fb_all as (
select fabd.ad_date, fabd.impressions, fabd.reach, fabd.clicks, 
fabd.leads, fabd.value, fabd.spend, fc.campaign_name, fa.adset_name,
'Facebook_ads' :: text as media_source
from facebook_ads_basic_daily fabd 
right join facebook_campaign fc 
on fc.campaign_id = fabd.campaign_id 
right join facebook_adset fa 
on fa.adset_id = fabd.adset_id),
fb_and_gl_all as(
select ad_date, impressions, reach, clicks, leads, value, spend, 
campaign_name, adset_name, media_source
from fb_all
union
select gabd.ad_date, gabd.impressions, gabd.reach, gabd.clicks,
gabd.leads, gabd.value, gabd.spend, 
gabd.campaign_name, gabd.adset_name,
'Google_ads' :: text as media_source
from google_ads_basic_daily gabd)
select campaign_name, adset_name, sum(spend) as tot_spend,
round (sum(value) ::numeric / sum(spend),2) as romi
from fb_and_gl_all
group by campaign_name, adset_name
having sum(spend) > 500000
order by romi desc 
limit 1;
**/
/**

with fb_all as (
select fabd.ad_date, fabd.impressions, fabd.reach, fabd.clicks, 
fabd.leads, fabd.value, fabd.spend, fc.campaign_name
from facebook_ads_basic_daily fabd 
right join facebook_campaign fc 
on fc.campaign_id = fabd.campaign_id)
select ad_date, campaign_name,
sum(spend) as tot_spend,
sum(impressions) as tot_impr,
sum(clicks) as tot_clicks,
sum(value) as tot_values,
sum(spend)/sum(clicks) as cpc,
1000 * sum(spend)/ sum(impressions) as cpm,
cast (sum(clicks) as numeric)/ sum (impressions) as ctr,
cast (sum(value) as numeric)/ sum(spend) as romi
from fb_all
where spend >0 and impressions >0 and clicks >0
group by ad_date, campaign_name;
**/

with fb_all as (
select fabd.ad_date, fabd.impressions,
fabd.reach,fabd.clicks, fabd.leads,fabd.value,
fabd.spend, fc.campaign_name, fa.adset_name,
fabd.url_parameters,
'Facebook_ads' :: text as media_source
from facebook_ads_basic_daily fabd 
right join facebook_campaign fc 
on fc.campaign_id = fabd.campaign_id 
right join facebook_adset fa 
on fa.adset_id = fabd.adset_id),
fb_and_gl_all as(
select ad_date, coalesce (impressions,0) as impressions,
coalesce (reach,0) as reach, coalesce (clicks,0) as clicks, 
coalesce (leads,0) as leads,
coalesce (value,0) as value, coalesce ( spend,0) as spend, 
campaign_name, adset_name, media_source, url_parameters
from fb_all
union
select gabd.ad_date, gabd.impressions, coalesce (gabd.reach,0) as reach,
coalesce (gabd.clicks,0) as clicks,
coalesce (gabd.leads,0) as leads,
coalesce (gabd.value,0) as value,
coalesce (gabd.spend,0) as spend,
gabd.campaign_name, gabd.adset_name,
'Google_ads' :: text as media_source, gabd.url_parameters
from google_ads_basic_daily gabd),
stat_by_month as(
select date_trunc('month', ad_date) as ad_month,
case 
	when lower(substring(url_parameters, 'utm_campaign=([\w|\d]+)')) = 'nan' then null
	else lower(substring(url_parameters, 'utm_campaign=([\w|\d]+)'))
end as utm_campaign,
sum(spend) as tot_spend,
sum(impressions) as tot_impr,
sum(clicks) as tot_clicks,
sum(value) as tot_values,
case 
	when sum (impressions) > 0 then round (cast (sum(clicks) as numeric)/ sum (impressions),3)
end as ctr,
case
    when sum (clicks)> 0 then sum(spend)/sum(clicks)
end as cpc,
case 
	when sum (impressions) > 0 then 1000 * sum(spend)/ sum(impressions)
end as cpm,
case 
	when sum(spend)> 0 then round(cast (sum(value) as numeric)/ sum(spend),3) 
end as romi
from fb_and_gl_all
group by ad_month, utm_campaign),
stat_by_lag as(
select ad_month, utm_campaign,
tot_spend, tot_impr, tot_clicks, tot_values,cpc,
ctr, cpm, romi,
lag (ctr, 1) over (partition by utm_campaign order by ad_month desc) as lag_ctr,
lag (cpm, 1) over (partition by utm_campaign order by ad_month desc) as lag_cpm,
lag (romi, 1) over (partition by utm_campaign order by ad_month desc) as lag_romi
from stat_by_month)
select ad_month, utm_campaign,
tot_spend, tot_impr, tot_clicks, tot_values,cpc,
ctr,
case 
	when ctr>0 then round(100*(ctr-lag_ctr)/ctr,2)
end as diff_ctr,
cpm, 100*(cpm-lag_cpm)/cpm as diff_cpm,
romi,
case 
	when romi>0 then round(100*(romi-lag_romi)/romi,2)
end as diff_romi
from stat_by_lag;

