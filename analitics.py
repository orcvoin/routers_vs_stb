import pandas as pd
from requests import post
import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 6})

a = ('spb', 'yola', 'voronezh', 'cheb', 'yar', 'oren', 'tver', 'chel', 'saratov', 'nn', 'kirov', 'samara', 'rostov',
     'mgn', 'ulsk', 'perm', 'volgograd', 'tmn', 'barnaul', 'bryansk', 'kursk', 'nch', 'krd', 'kurgan', 'lipetsk',
     'tula', 'irkutsk', 'izhevsk', 'tomsk', 'engels', 'ulu', 'kzn', 'ekat', 'penza', 'krsk', 'kazan', 'nk', 'ryazan',
     'ufa', 'omsk', 'novosib', 'chelny', 'nsk')

sql_routers = """
select distinct
agreement,
pv_model_name as model
from
spas.collected_statistics
where create_date >= today()-5 and create_date < today()
and pv_model_name in (
'H118NV2.1',
'Archer C5',
'H118N',
'Archer C20',
'WNR612-2EMRUS',
'RX-22200',
'JWNR2000-4EMRUS',
'Sag',
'H218N',
'DIR-615',
'EVG1500',
'RX-22301',
'S1010',
'DIR-300',
'EC330',
'DIR-615A',
'W4N(rev.M)',
'H198A',
'Nonexistent model'
)
FORMAT JSON;
"""

sql_agr_pro = """
select distinct
agreement,
terminal_resource,
platform,
domain,
countIf(value[indexOf(key,'error.code_ui')] <> '') as count_error,
uniqIf(terminal_resource, value[indexOf(key,'error.code_ui')] <> '') as uniq_error,
value[indexOf(key,'error.code_ui')] as error,
count(terminal_resource) as count,
uniq(terminal_resource) as uniq,
count_error/ count*100 as error_count_percent
from
spas.tv_devices_log
where
log_entry_date >= today()-5 and log_entry_date < today()
group by agreement, terminal_resource, platform, domain, error
FORMAT JSON;
"""

sql_bitrate = """
select distinct
agreement,
domain,
device_id,
count(device_id) as device_count,
multiIf(
    platformX like '%ERT IPTV', 'iptv',
    platformX like '%STB30', 'stb30',
    platformX like '%IOS', 'ios',
    platformX like '%Android', 'android',
    platformX like '%HUMAX7000', 'humax7000',
    (platformX like '%SAMSUNG' or platformX like '%LG'), 'SmartTV',
    platformX like '%OTTWeb', 'ottweb',
    platformX like '%STB', 'stb',
    platformX like '%ANDROID IPTV', 'android_iptv',
    'other') as platform,
bitrate,
multiIf(
    platformX like '%ERT IPTV' and bitrate < 4000000, 1, 
    platformX like '%STB30' and bitrate < 4000000, 1,
    platformX like '%IOS' and bitrate < 1000000, 1,
    platformX like '%Android' and bitrate < 1000000, 1,
    platformX like '%HUMAX7000' and bitrate < 4000000, 1,
    (platformX like '%SAMSUNG' or platformX like '%LG') and bitrate < 2000000, 1,
    platformX like '%OTTWeb' and bitrate < 1000000, 1,
    platformX like '%STB' and bitrate < 4000000, 1,
    platformX like '%ANDROID IPTV' and bitrate < 4000000, 1,
0) as bitrate_bad
from (
    SELECT
        agreement,
        domain,
        device_id,
    	dictGetString('platform', 'name', toUInt64(platform_id)) as platformX,
    	avg(content_bandwidth) as bitrate
    FROM
    	spas.cdn_raw_v1
    where
 		log_entry_date >= today()-5 and log_entry_date < today()
 		and platform_id != 0 and request_completion = 1 and channel_type = 'hd'
 		and domain in {a}
    group by agreement, platformX, domain, device_id
)
group by agreement, domain, device_id, platformX, bitrate
FORMAT JSON;
"""

sql_buffers = """
select distinct
agreement,
terminal_resource,
platform,
domain,
countIf(value[indexOf(key,'state')] = 'ERROR') as count_state_error,
countIf(value[indexOf(key, 'state')] = 'BUFFERING') as count_state_buffer,
uniqIf(terminal_resource, value[indexOf(key,'state')] = 'ERROR') as uniq_state_error,
uniqIf(terminal_resource, value[indexOf(key, 'state')] = 'BUFFERING') as uniq_state_buffer,
count() as count,
uniq(terminal_resource) as uniq,
count_state_buffer/count*100 as buffer_percent,
count_state_error/count*100 as error_percent
from
spas.tv_devices_log
where log_entry_date >= today()-5 and log_entry_date < today()
and domain in {a}
group by agreement, terminal_resource, platform, domain
FORMAT JSON;
"""


sql_cdn_device_request = """
select distinct
device_id,
countIf(request_time > 5) as count_request_time,
uniqIf(device_id, request_time > 5) as uniq_request_time,
count(device_id) as count,
uniq(device_id) as uniq,
domain
from
spas.cdn_raw_v1
where log_entry_date >= today()-5 and log_entry_date < today()
    and request_upstream_response_time <> 9999
    and device_id <> 0
    and domain in {a}
group by device_id, domain
FORMAT JSON;
"""

sql_balancer_device = """
select distinct
subscriber_ext_id_number as agreement,
platform_ext_id as platform,
device_id
from
spas.backend_balancer_raw
where log_entry_date >= today()-5 and log_entry_date < today()
FORMAT JSON;
"""

# # # Выгрузка роутеры
res = pd.DataFrame(columns=['agreement','model'])
url_result = post(url='http://spas.ertelecom.ru:8123', data=sql_routers.encode('utf-8'))
data = pd.DataFrame.from_records(url_result.json().get('data', []))
res = pd.merge(res, data, on=['agreement','model'],how='outer')
# # # Выгрузка ошибки
res_2 = pd.DataFrame(columns=['agreement','terminal_resource','platform','domain','count_error','count','uniq','error_count_percent','error','uniq_error'])
url_result_2 = post(url='http://spas.ertelecom.ru:8124', data=sql_agr_pro.format(a=a).encode('utf-8'))
data_2 = pd.DataFrame.from_records(url_result_2.json().get('data', []))
res_2 = pd.merge(res_2, data_2, on=['agreement','terminal_resource','platform','domain','count_error','count','uniq','error_count_percent','error','uniq_error'],how='outer')
result = pd.merge(res, res_2, on=['agreement'], how='inner')
print(result.shape)
result['bad_rate'] = result['count_error'].astype(str).astype(int)/result['count'].astype(str).astype(int)
# # # print(result)

bad_rate = result.groupby(['model'])['bad_rate'].sum()
county = result.groupby(['model'])['agreement'].count()
# result.drop(columns=['agreement','terminal_resource','platform','domain','uniq','error_count_percent','error','uniq_error'], inplace=True)
final = bad_rate / county
final.plot.bar(rot=25)
plt.title('Количество ошибок на уникального пользователя')
plt.savefig('ui_errors.png')
# plt.show()
# # # Выгрузка битрейт
res_3 = pd.DataFrame(columns=['agreement','domain','device_id','device_count','platform','bitrate','bitrate_bad'])
url_result_3 = post(url='http://spas.ertelecom.ru:8124', data=sql_bitrate.format(a=a).encode('utf-8'))
data_3 = pd.DataFrame.from_records(url_result_3.json().get('data', []))
res_3 = pd.merge(res_3, data_3, on=['agreement','domain','device_id','device_count','platform','bitrate','bitrate_bad'],how='outer')
result_bitrate = pd.merge(res, res_3, on=['agreement'], how='inner')
print(result_bitrate.shape)
# # # print(result_bitrate)
result_bitrate['bad_rate'] = result_bitrate['bitrate_bad'].astype(str).astype(int)/result_bitrate['device_count'].astype(str).astype(int)
# # # print(result_bitrate)
bad_bitrate = result_bitrate.groupby(['model'])['bad_rate'].sum()
county_bitrate = result_bitrate.groupby(['model'])['agreement'].count()
final_bitrate = bad_bitrate / county_bitrate
final_bitrate.plot.bar(rot=25)
plt.title('Количество плохих битрейтов на уникального пользователя')
plt.savefig('bad_bitrate.png')
# plt.show()
# # # Выгрузка буферизации
res_4 = pd.DataFrame(columns=['agreement','terminal_resource','platform','domain','count','uniq','count_state_buffer','count_state_error','uniq_state_error','uniq_state_buffer'])
url_result_4 = post(url='http://spas.ertelecom.ru:8124', data=sql_buffers.format(a=a).encode('utf-8'))
data_4 = pd.DataFrame.from_records(url_result_4.json().get('data', []))
res_4 = pd.merge(res_4, data_4, on=['agreement','terminal_resource','platform','domain','count','uniq','count_state_buffer','count_state_error','uniq_state_error','uniq_state_buffer'],how='outer')
result_buffers = pd.merge(res, res_4, on=['agreement'], how='inner')
print(result_buffers.shape)
result_buffers['bad_errors_rate'] = result_buffers['count_state_buffer'].astype(str).astype(int)/result_buffers['count'].astype(str).astype(int)
result_buffers['bad_buffers_rate'] = result_buffers['count_state_error'].astype(str).astype(int)/result_buffers['count'].astype(str).astype(int)
# # # print(result_buffers)
bad_result_buffers = result_buffers.groupby(['model'])['bad_buffers_rate'].sum()
bad_result_errors = result_buffers.groupby(['model'])['bad_errors_rate'].sum()
county_buffers = result_buffers.groupby(['model'])['agreement'].count()
final_buffers = bad_result_buffers / county_buffers
final_errorss = bad_result_errors / county_buffers
final_buffers.plot.bar(rot=25)
plt.title('Количество буферизаций плеера на уникального пользователя')
plt.savefig('bad_buffers_rate.png')
final_errorss.plot.bar(rot=25)
plt.title('Количество ошибок плеера на уникального пользователя')
plt.savefig('bad_errors_rate.png')
# plt.show()
# # # # Выгрузка высокий апстрим CDN
res_5 = pd.DataFrame(columns=['device_id','domain','count_request_time','count','uniq','uniq_request_time'])
url_result_5 = post(url='http://spas.ertelecom.ru:8124', data=sql_cdn_device_request.format(a=a).encode('utf-8'))
data_5 = pd.DataFrame.from_records(url_result_5.json().get('data', []))
res_5 = pd.merge(res_5, data_5, on=['device_id','domain','count_request_time','count','uniq','uniq_request_time'],how='outer')
res_5['device_id'] = res_5['device_id'].astype(str).astype(int)
# # # print(res_5.shape)
# # # Выгрузка device_id с балансера
res_6 = pd.DataFrame(columns=['device_id','agreement','platform'])
url_result_6 = post(url='http://spas.ertelecom.ru:8124', data=sql_balancer_device.encode('utf-8'))
data_6 = pd.DataFrame.from_records(url_result_6.json().get('data', []))
res_6 = pd.merge(res_6, data_6, on=['device_id','agreement','platform'],how='outer')
res_6['device_id'] = res_6['device_id'].astype(str).astype(int)
result_bal_cdn = pd.merge(res_5, res_6, on=['device_id'], how='inner')
# # print(res_6.shape)
print(result_bal_cdn.shape)
# # # мердж данных (cdn+balancer) с роутерами
result_request_time = pd.merge(res, result_bal_cdn, on=['agreement'], how='inner')
print(result_request_time.shape)
result_request_time['bad_request_time'] = result_request_time['count_request_time'].astype(str).astype(int)/result_request_time['count'].astype(str).astype(int)
bad_result_request_time = result_request_time.groupby(['model'])['bad_request_time'].sum()
county_request_time = result_request_time.groupby(['model'])['agreement'].count()
final_request_time = bad_result_request_time / county_request_time
final_request_time.plot.bar(rot=25)
plt.title('Количество bad_request_time на уникального пользователя')
plt.savefig('bad_request_time.png', fontsize=10)
# plt.show()
