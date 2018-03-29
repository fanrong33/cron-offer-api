<?php
/**
 * 拉取Clickdealer CPA Offer API任务计划（MongoDB版）
 * @changelog 修复可能接口请求失败，导致全部下架的bug
 *            优化Offer唯一标识逻辑，添加Aff ID
 *            简化函数逻辑
 * @version 1.3.9 build 20180329
 */

/*
 * 每次刷新api, 需要判断已入库的，如果api没有再返回，需要自动把正式数据表里的停掉；
 * 如果有返回，需要同步更新信息及状态，保持信息一致。
 * 如果还没有入库，则仅同步更新筛选数据表的信息。
 */

// vi /etc/crontab
// 26,56  *  *  *  * root /usr/bin/php /data/www/command_jc/OfferApi/cron_clickdealer_cpa_offer_mongo.php 2>&1 >> /dev/null &
error_reporting(0);
set_time_limit(0);
date_default_timezone_set('PRC');

define('DEBUG_MODE', true);
define('ENABLED_AUTO_UPDATE', true);

require_once dirname(__FILE__).'/../Core/Clickdealer.class.php';
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


$clickdealer = new Clickdealer($config['CLICKDEALER_AFF_ID'], $config['CLICKDEALER_API_KEY']);
$aff_id     = $config['CLICKDEALER_AFF_ID'];
$network_id = 46;

echo sprintf("------------------------------------ \n");
$timestamp = time();
$current_date = date('c', $timestamp);
echo sprintf("当前时间: %s \n", $current_date);





// 传入page参数，以此反复，直至返回{"errno":5,"errmsg":"there is not offer","data":[]}为止。
// {"errno":0,"errmsg":"success",
$api_request_status = false;
$offer_list = array();


function get_offer_list_via_page($page, $limit, $clickdealer, &$offer_list, &$api_request_status){
    $params = array(
        'vertical_id'     => '115,101', // 订阅offer
        'connection_type' => 0,
        'demo'            => 0,
        'flow'            => 0,
        'offer_status_id' => 0,
        'tracking_type'   => 2,
        'start_at_row'    => ($page-1)*$limit+1,
        'row_limit'       => $limit,
    );

    $json = $clickdealer->offer_feed($params);
    if($json['success'] == 'true'){
        echo sprintf("1、调用Clickdealer Offer API接口成功 \n");

        $api_request_status = true;


        $total_pages = ceil($json['row_count']/$limit);
        echo sprintf("page: %s, limit: %s, total_pages: %s \n", $page, $limit, $total_pages);

        // 循环判断offer状态, 看是否需要进行开通offer
        $offer_id_list = array();
        foreach ($json['offers'] as $rs) {
            if($rs['offer_status']['status_id'] != 3){
                $params = array(
                    'offer_contract_id'      => $rs['offer_contract_id'],
                    'media_type_id'          => 3,
                    'notes'                  => 0,
                    'agreed_to_terms'        => 'TRUE',
                    'agreed_from_ip_address' => '11.11.11.11',
                );
                $e = $clickdealer->apply_for_offer($params);
                if($e['success'] == 'true'){
                    $offer_id_list[] = $rs['offer_id'];
                }
            }
            if($rs['offer_status']['status_id'] == 3){
                $offer_id_list[] = $rs['offer_id'];
            }
        }


        // 使用已通过的offer id，批量获取完整offer信息（最多100个）
        $params = array(
            'offer_id' => join($offer_id_list, ',')
        );
        $json = $clickdealer->get_campaign($params);

        if($json['success'] == 'true'){
            unset($json['row_count']);
            unset($json['success']);
            foreach ($json as $key => $offer) {

                $offer_list[] = $offer;

            }
        }
        if($total_pages > $page){
            unset($json);
            $page++;
            get_offer_list_via_page($page, $limit, $clickdealer, $offer_list, $api_request_status);
        }
    }else{
        var_dump($json);
        echo sprintf("[error] 调用Clickdealer Offer API接口失败。 msg: %s \n", $json['msg']);
        $api_request_status = false;
    }
}

$page  = 1;
$limit = 100; // TODO 正式环境修改为大一点，总5600
get_offer_list_via_page($page, $limit, $clickdealer, $offer_list, $api_request_status);
// var_dump($offer_list);
// echo "***\n";
// exit;


// 定义api接口返回的offer id列表，用于暂停未返回的
$api_offerid_list = array();
if($offer_list){
    echo sprintf("{ offers_count: %d } \n", count($offer_list));
    echo sprintf("2、循环判断返回的offer是否已存在临时库中，如果有，则匹配更新字段；如果没有，则添加 \n");
    foreach ($offer_list as $key => $rs) {

        $thirdparty_offer_id = $rs['offer_id'].'';

        $api_offerid_list[] = $thirdparty_offer_id;

        // 判断是否在临时库，如果有，则匹配更新字段; 如果没有，则添加。
        $offer_api = get_offer_api($thirdparty_offer_id, $network_id, $aff_id, $db, $redis);
        if(!$offer_api){
            echo sprintf("%s, %d, %s 还不存在临时库中，开始添加到临时库 \n", $thirdparty_offer_id, $network_id, $aff_id);

            // !!!
            // {
            //     "flow_id": 5,
            //     "flow_name": "cpl (soi\/email submit)"
            // }
            // {
            //     "flow_id": 12,
            //     "flow_name": "dcb (1-click flow)"
            // }
            // {
            //     "flow_id": 13,
            //     "flow_name": "dcb (2-click flow)"
            // }
            // {
            //     "flow_id": 14,
            //     "flow_name": "pin submit (mo flow)"
            // }
            // {
            //     "flow_id": 15,
            //     "flow_name": "pin submit (mt flow)"
            // }
            // {
            //     "flow_id": 16,
            //     "flow_name": "pin submit (click2sms)"
            // }
            $conversion_map = array(
                5  => array('CPA', ''),
                12 => array('CPA', 'One Click'),
                13 => array('CPA', 'Two Click'),
                14 => array('CPA', 'PIN'),
                15 => array('CPA', 'PIN'),
                16 => array('CPA', 'PIN'),
            );

            if($rs['flows']){
                $flow_id = $rs['flows'][0]['flow_id'];

                $payout_type     = $conversion_map[$flow_id][0];
                $conversion_flow = $conversion_map[$flow_id][1];
            }

            $connector_type = '3G,4G';
            // $creative_list = array();
            // if($rs['creative_files']){
            //     foreach ($rs['creative_files'] as $creative_file) {
            //         $creative_list[] = $creative_file['link'];
            //     }
            // }
            $country_code_list = array();
            foreach ($rs['allowed_countries'] as $item) {
                $country_code_list[] = strtoupper($item['country_code']);
            }

            $data = array(
                'thirdparty_offer_id' => $rs['offer_id'].'',
                'name'                => $rs['offer_name'],
                'network_id'          => $network_id,
                'aff_id'              => $aff_id,
                'app_name'            => '',
                'app_desc'            => '',
                'url'                 => $rs['creatives'][0]['unique_link'],
                'icon_link'           => '',
                'country_code'        => join($country_code_list, ','),
                'platform'            => 'ios,android', // CPA 不需要区分平台，订阅类型
                'description'         => $rs['description'], 
                'kpi_info'            => $rs['restrictions'],
                'category'            => '',
                'min_version'         => '',
                'package_name'        => '',
                'preview_link'        => $rs['preview_link'],
                'payout_type'         => 'CPA',
                'conversion_flow'     => $conversion_flow,
                'carriers'            => '',
                'connector_type'      => $connector_type,

                'price'               => floatval($rs['payout_converted']),
                'creatives'           => '',
                'cvn_cap'             => 999999,
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
            $new_price   = floatval($rs['payout_converted']);
            $new_cvn_cap = 999999;

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