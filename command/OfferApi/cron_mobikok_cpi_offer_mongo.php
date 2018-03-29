<?php
/**
 * 拉取Mobikok CPI Offer API任务计划（MongoDB版）
 * @changelog 修复可能接口请求失败，导致全部下架的bug
 *            优化Offer唯一标识逻辑，添加Aff ID
 *            简化函数逻辑
 * @version 1.3.7 build 20180229
 */

/*
 * 每次刷新api, 需要判断已入库的，如果api没有再返回，需要自动把正式数据表里的停掉；
 * 如果有返回，需要同步更新信息及状态，保持信息一致。
 * 如果还没有入库，则仅同步更新筛选数据表的信息。
 */

// 28,58  *  *  *  * root /usr/bin/php /data/www/command_jc/OfferApi/cron_mobikok_cpi_offer_mongo.php 2>&1 >> /dev/null &
error_reporting(0);
set_time_limit(0);
date_default_timezone_set('PRC');
define('DEBUG_MODE', true);
define('ENABLED_AUTO_UPDATE', true);

require_once dirname(__FILE__).'/../Core/Mobikok.class.php';
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


$mobikok = new Mobikok($config['MOBIKOK_AFF_ID'], $config['MOBIKOK_API_TOKEN']);
$aff_id     = $config['MOBIKOK_AFF_ID'];
$network_id = 88;


echo sprintf("------------------------------------ \n");
$timestamp = time();
$current_date = date('c', $timestamp);
echo sprintf("当前时间: %s \n", $current_date);





// 传入page参数，以此反复，直至返回{"errno":5,"errmsg":"there is not offer","data":[]}为止。
// {"errno":0,"errmsg":"success",
$api_request_status = false;
$offer_list = array();


function get_offer_list_via_page($page, $limit, $mobikok, &$offer_list, &$api_request_status){
    $params = array(
        'isRecommend' => 1,
        'limit' => $limit
    );
    $json = $mobikok->get_offer($params);

    if($json['status'] == 'success'){
        echo sprintf("1、调用Mobikok Offer API接口成功 \n");

        $api_request_status = true;

        $total = $json['total'];
        echo sprintf("page: %s, limit: %s, total: %s \n", $page, $limit, $total);

        foreach ($json['offers'] as $key => $offer) {

            $offer_list[] = $offer;

        }
        // Mobikok 没有分页功能
        // if($total_pages > $page){
        //     unset($json);
        //     $page++;
        //     get_offer_list_via_page($page, $limit, $mobikok, $offer_list, $api_request_status);
        // }
    }else{
        echo sprintf("[error] 调用Mobikok Offer API接口失败。 msg: %s \n", $json['msg']);
        $api_request_status = false;
    }
}

$page  = 1;
$limit = 3000;  // 3000
get_offer_list_via_page($page, $limit, $mobikok, $offer_list, $api_request_status);
// var_dump($offer_list);
// echo "***\n";
// exit;


// 定义api接口返回的offer id列表，用于暂停未返回的
$api_offerid_list = array();
if($offer_list){
    echo sprintf("{ offers_count: %d } \n", count($offer_list));
    echo sprintf("2、循环判断返回的offer是否已存在临时库中，如果有，则匹配更新字段；如果没有，则添加 \n");
    foreach ($offer_list as $key => $rs) {

        $thirdparty_offer_id = $rs['id'].'';

        $api_offerid_list[] = $thirdparty_offer_id;

        // 判断是否在临时库，如果有，则匹配更新字段; 如果没有，则添加。
        $offer_api = get_offer_api($thirdparty_offer_id, $network_id, $aff_id, $db, $redis);
        if(!$offer_api){
            echo sprintf("%s, %d, %s 还不存在临时库中，开始添加到临时库 \n", $thirdparty_offer_id, $network_id, $aff_id);


            $connector_type = '3G,4G,WiFi';


            $url = $rs['trackingUrl'];
            $url = substr($url, 0, strpos($url, 's1')-1);

            $country_code_list = array();
            foreach ($rs['area'] as $item) {
                $country_code_list[] = strtoupper($item['country']);
            }

            $platform_list = array();
            if(in_array('iPhone', $rs['device'])){
                $platform_list[] = 'ios';
            }
            if(in_array('Android', $rs['device'])){
                $platform_list[] = 'android';
            }

            if($rs['preview']){
                if(in_array('ios', $platform_list)){
                    preg_match('/id(\d*)\?/', $rs['preview'], $matches);
                    $package_name = $matches[1];
                }
                if(in_array('android', $platform_list)){
                    preg_match('/id=([A-Za-z0-9_.]+)/', $rs['preview'], $matches);
                    $package_name = $matches[1];
                }
            }

            $data = array(
                'thirdparty_offer_id' => $rs['id'].'',
                'name'                => $rs['name'],
                'network_id'          => $network_id,
                'aff_id'              => $aff_id,
                'app_name'            => '',
                'app_desc'            => '',
                'url'                 => $url,
                'icon_link'           => '',
                'country_code'        => join($country_code_list, ','),
                'platform'            => join($platform_list, ','), // CPA 不需要区分平台，订阅类型
                'description'         => $rs['description'],
                'kpi_info'            => '',
                'category'            => $rs['category'],
                'min_version'         => $rs['min_os_version'],
                'package_name'        => $package_name,
                'preview_link'        => $rs['preview'],
                'payout_type'         => 'CPI',
                'conversion_flow'     => 'CPI',
                'carriers'            => '',
                'connector_type'      => $connector_type,

                'price'               => floatval($rs['payout']),
                'creatives'           => '',
                'cvn_cap'             => $rs['caps'],
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
            $new_price   = floatval($rs['payout']);
            $new_cvn_cap = $rs['caps'];

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