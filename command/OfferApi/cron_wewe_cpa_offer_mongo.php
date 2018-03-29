<?php
/**
 * 拉取Wewe CPA Offer API任务计划（MongoDB版）
 * @changelog 修复可能接口请求失败，导致全部下架的bug
 *            优化Offer唯一标识逻辑，添加Aff ID
 *            简化函数逻辑
 * @version 1.3.8 build 20180329
 */

/*
 * 每次刷新api, 需要判断已入库的，如果api没有再返回，需要自动把正式数据表里的停掉；
 * 如果有返回，需要同步更新信息及状态，保持信息一致。
 * 如果还没有入库，则仅同步更新筛选数据表的信息。
 */

// vi /etc/crontab
// 24,54  *  *  *  * root /usr/bin/php /data/www/command_jc/OfferApi/cron_wewe_cpa_offer_mongo.php 2>&1 >> /dev/null &
error_reporting(0);
set_time_limit(0);
date_default_timezone_set('PRC');
define('DEBUG_MODE', true);
define('ENABLED_AUTO_UPDATE', true);

require_once dirname(__FILE__).'/../Core/Wewe.class.php';
require_once dirname(__FILE__).'/../Common/common.php';

if(DEBUG_MODE){
    $config = include dirname(__FILE__)."/../Conf/debug.php";
}else{
    $config = include dirname(__FILE__)."/../Conf/config.php";
}


$redis = new Redis(); 
$connected = $redis->connect($config['REDIS_HOST'], $config['REDIS_PORT']);
if($connected) $redis->auth($config['REDIS_PWD']);

$mongo = new MongoClient($config['MONGODB_SERVER']);
$db = $mongo->$config['MONGODB_NAME'];


$wewe = new Wewe($config['WEWE_API_KEY']);
$aff_id     = $config['WEWE_AFF_ID'];
$network_id = 86;

echo sprintf("------------------------------------ \n");
$timestamp = time();
$current_date = date('c', $timestamp);
echo sprintf("当前时间: %s \n", $current_date);





// 传入page参数，以此反复，直至返回{"errno":5,"errmsg":"there is not offer","data":[]}为止。
// {"errno":0,"errmsg":"success",
$api_request_status = false;
$offer_list = array();


function get_offer_list_via_page($page, $limit, $wewe, &$offer_list, &$api_request_status){
    $params = array(
        'page'  => $page,
        'limit' => $limit,
    );
    $json = $wewe->get_offer($params);
    if($json){
        echo sprintf("1、调用Wewe Offer API接口成功 \n");

        $api_request_status = true;

        echo sprintf("page: %s, limit: %s \n", $page, $limit);

        foreach ($json['data'] as $key => $offer) {

            $offer_list[] = $offer;

        }
        if($json['meta']['is_complete'] == false){
            unset($json);
            $page++;
            get_offer_list_via_page($page, $limit, $wewe, $offer_list, $api_request_status);
        }
    }else{
        echo sprintf("[error] 调用Wewe Offer API接口失败。 msg: %s \n", $json['msg']);
        $api_request_status = false;
    }
}

$page  = 1;
$limit = 100;
get_offer_list_via_page($page, $limit, $wewe, $offer_list, $api_request_status);


// 定义api接口返回的offer id列表，用于暂停未返回的
$api_offerid_list = array();
if($offer_list){
    echo sprintf("{ offers_count: %d } \n", count($offer_list));
    echo sprintf("2、循环判断返回的offer是否已存在临时库中，如果有，则匹配更新字段；如果没有，则添加 \n");
    foreach ($offer_list as $key => $rs) {
        $thirdparty_offer_id = $rs['affiliate_camp_id'].'';

        $api_offerid_list[] = $thirdparty_offer_id;

        // 判断是否在临时库，如果有，则匹配更新字段; 如果没有，则添加。
        $offer_api = get_offer_api($thirdparty_offer_id, $network_id, $aff_id, $db, $redis);

        if(!$offer_api){
            echo sprintf("%s, %d, %s 还不存在临时库中，开始添加到临时库 \n", $thirdparty_offer_id, $network_id, $aff_id);

            $payout_type = 'CPA';
            $conversion_flow = '';
            if(stristr($rs['campaign_name'], '1Click')){
                $conversion_flow = 'One Click';
            }elseif(stristr($rs['campaign_name'], '2Click')){
                $conversion_flow = 'Two Click';
            }


            $connector_type = '3G,4G';


            // 最后一个字符为&, 则去掉
            $url = $rs['click_link'];
            $url = substr($url, 0, strpos($url, 'pubid')-1);

            $data = array(
                'thirdparty_offer_id' => $rs['affiliate_camp_id'].'',
                'name'                => $rs['campaign_name'],
                'network_id'          => $network_id,
                'aff_id'              => $aff_id,
                'app_name'            => $rs['campaign_app_name'] ? $rs['campaign_app_name'] : '',
                'app_desc'            => $rs['campaign_app_description'],
                'url'                 => $url,
                'icon_link'           => '',
                'country_code'        => $rs['campaign_country_codes'],
                'platform'            => 'ios,android', // CPA 不需要区分平台，订阅类型
                'description'         => $rs['campaign_description'], 
                'kpi_info'            => '',
                'category'            => '',
                'min_version'         => '',
                'package_name'        => '',
                'preview_link'        => '',
                'payout_type'         => 'CPA',
                'conversion_flow'     => $conversion_flow ? $conversion_flow : '',
                'carriers'            => $rs['tags'],
                'connector_type'      => $connector_type,

                'price'               => floatval($rs['campaign_traffic_cost']),
                'creatives'           => '',
                'cvn_cap'             => $rs['daily_cap_remaining']=='Unlimited' ? 999999 : $rs['daily_cap_remaining'],
                'deviceid_must'       => -1,

                'is_imported'         => 0,
                'is_actived'          => 1,
                'update_time'         => time(),
                'create_time'         => time(),
            );
            
            $response = $db->offer_apis->insert($data);
            if($response['ok']){
                echo sprintf("添加OfferApi成功 \n");
            }else{
                echo sprintf("[error] 添加OfferApi失败 \n");
            }

        }else{
            echo sprintf("%s, %d, %s 已存在临时库中，开始更新匹配临时库 \n", $thirdparty_offer_id, $network_id, $aff_id);
            
            // 若存在offer_api库中，只判断api接口中的主要部分数据是否发生变化，再来更新offer_api
            $new_price   = floatval($rs['campaign_traffic_cost']);
            $new_cvn_cap = $rs['daily_cap_remaining']=='Unlimited' ? 999999 : $rs['daily_cap_remaining'];

            update_offer_api($new_price, $new_cvn_cap, $offer_api, $thirdparty_offer_id, $network_id, $aff_id, $db, $redis);


            if(ENABLED_AUTO_UPDATE){
                echo sprintf("  %s,%d,%s 判断是否已存在正式库中，如果有，则匹配更新字段。 \n", $thirdparty_offer_id, $network_id, $aff_id);
                update_offer($new_price, $new_cvn_cap, $thirdparty_offer_id, $network_id, $aff_id, $db);
            }// if(ENABLED_AUTO_UPDATE){
        }
    }

}

// 接口请求成功，才进行处理，防止误伤（接口请求失败，结果把正式库Offer全部都停掉了）
if($api_request_status){

    // 需要判断已入库的，如果api没有再返回，需要自动把临时数据表里的暂停
    echo sprintf("3、如果api没有再返回，需要自动把OfferApi数据表里的停掉 \n");
    pause_offer_api($api_offerid_list, $network_id, $aff_id, $db, $redis);


    if(ENABLED_AUTO_UPDATE){
        echo sprintf("4、判断已入库的，如果api没有再返回，需要自动把正式数据表里的停掉 \n");
        pause_offer($api_offerid_list, $network_id, $aff_id, $db);
    } // if(ENABLED_AUTO_UPDATE){

} // if($api_request_status){

$redis->close();

?>