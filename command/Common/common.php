<?php

/**
 * 获取offer api信息
 * @param  integer   $thirdparty_offer_id
 * @param  integer   $network_id
 * @param  resource  $mysql
 * @param  resource  $ssdb
 * @return array
 */
function get_offer_api($thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb){
    // 从ssdb获取offer信息，从offer信息得到user_id
    $cache_key = $thirdparty_offer_id.'_'.$network_id.'_'.$aff_id;
    $offer_json = $ssdb->hget('offer_apis', $cache_key);
    if($offer_json){
        $offer_api = json_decode($offer_json, true);
    }else{
        $offer_api = $mysql->executeSQL("select * from t_offer_api where thirdparty_offer_id='{$thirdparty_offer_id}' and network_id={$network_id} and aff_id='{$aff_id}' ");
        if(is_array($offer_api)){
            $ssdb->hset('offer_apis', $cache_key, json_encode($offer_api));
        }else{
            $offer_api = false;
        }
    }
    return $offer_api;
}

function get_offer_api2($thirdparty_offer_id, $network_id, $aff_id, $db, $ssdb){
    // 从ssdb获取offer信息，从offer信息得到user_id
    $cache_key = 'offer_apis_'.$thirdparty_offer_id.'_'.$network_id.'_'.$aff_id;
    $offer_json = $ssdb->get($cache_key);
    $offer_json = null;
    if($offer_json){
        $offer_api = json_decode($offer_json, true);
    }else{

        $where = array('$and' => array( array('thirdparty_offer_id'=>$thirdparty_offer_id), array('network_id'=>$network_id), array('aff_id'=>$aff_id) ));

        $offer_api = $db->offer_apis->findOne($where);
        if($offer_api){
            $ssdb->setx($cache_key, json_encode($offer_api), 3600); // 设置缓存1小时
        }
    }
    return $offer_api;
}



/**
 * 获取offer信息
 * @param  integer   $offer_id
 * @param  integer   $network_id
 * @param  resource  $mysql
 * @param  resource  $ssdb
 * @return array
 */
function get_offer($thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb){
    // 从ssdb获取offer信息，从offer信息得到user_id
    $cache_key = $thirdparty_offer_id.'_'.$network_id.'_'.$aff_id;
    $offer_json = $ssdb->hget('offers', $cache_key);
    if($offer_json){
        $offer = json_decode($offer_json, true);
    }else{
        $offer = $mysql->executeSQL("select * from t_offer where thirdparty_offer_id='{$thirdparty_offer_id}' and network_id={$network_id} and aff_id='{$aff_id}'");
        if(is_array($offer)){
            $ssdb->hset('offers', $cache_key, json_encode($offer));
        }else{
            $offer = false;
        }
    }
    return $offer;
}

function get_offer2($thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb){
    // 从ssdb获取offer信息，从offer信息得到user_id
    $cache_key = 'offers_'.$thirdparty_offer_id.'_'.$network_id.'_'.$aff_id;
    $offer_json = $ssdb->get($cache_key);
    if($offer_json){
        $offer = json_decode($offer_json, true);
    }else{
        $offer = $mysql->executeSQL("select * from t_offer where thirdparty_offer_id='{$thirdparty_offer_id}' and network_id={$network_id} and aff_id='{$aff_id}'");
        if(is_array($offer)){
            $ssdb->setx($cache_key, json_encode($offer), 3600);
        }else{
            $offer = false;
        }
    }
    return $offer;
}


function update_offer_api($new_price, $new_cvn_cap, $offer_api, $thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb){
    $api_data = array(
        'price'      => $new_price,
        'cvn_cap'    => $new_cvn_cap,
        'is_actived' => 1,
    );
    $offer_api_data = array(
        'price'      => floatval($offer_api['price']),
        'cvn_cap'    => $offer_api['cvn_cap'],
        'is_actived' => $offer_api['is_actived'],
    );

    if($thirdparty_offer_id == 'h5a583514b6b0a'){
        // var_dump($api_data);
        // var_dump($offer_api_data);
        // exit;
    }

    if(http_build_query($api_data) != http_build_query($offer_api_data)){
        echo sprintf("  存在变化，开始进行更新OfferApi \n");

        $price      = $new_price;
        $cvn_cap    = $new_cvn_cap;
        $is_actived = 1;
        $time       = time();

        $sql =<<<EOF
            UPDATE `t_offer_api` SET  `price`={$price}, `cvn_cap`={$cvn_cap}, `is_actived`=$is_actived, `update_time`={$time}  WHERE `id`={$offer_api['id']}
EOF;
        $effect = $mysql->executeSQL($sql);
        if($effect){
            echo sprintf("  更新OfferApi成功 \n");

            // 删除SSDB offer_api 缓存
            $ssdb->hdel('offer_apis', $thirdparty_offer_id.'_'.$network_id.'_'.$aff_id);
        }else{
            echo sprintf("  [error] 更新OfferApi失败 \n");
        }

    }else{
        echo sprintf("  不存在变化，不需要更新OfferApi \n");
    }
}


function update_offer_api2($new_price, $new_cvn_cap, $offer_api, $thirdparty_offer_id, $network_id, $aff_id, $db, $ssdb){
    $api_data = array(
        'price'      => $new_price,
        'cvn_cap'    => $new_cvn_cap,
        'is_actived' => 1,
    );
    $offer_api_data = array(
        'price'      => floatval($offer_api['price']),
        'cvn_cap'    => $offer_api['cvn_cap'],
        'is_actived' => $offer_api['is_actived'],
    );
    if(http_build_query($api_data) != http_build_query($offer_api_data)){
        
        echo sprintf("  存在变化，开始进行更新OfferApi \n");

        $price      = $new_price;
        $cvn_cap    = $new_cvn_cap;
        $is_actived = 1;
        $time       = time();

        // $mongoID = new MongoID($offer_api['_id']['$id']);
        // $where = array('_id' => $mongoID);
        $where = array('thirdparty_offer_id'=>$thirdparty_offer_id, 'network_id'=>$network_id, 'aff_id'=>$aff_id);
        $update = array('$set' => array('price' => $price, 'cvn_cap'=>$cvn_cap, 'is_actived'=>$is_actived, 'update_time'=>$time));
        $response = $db->offer_apis->update($where, $update, array('upsert'=>false, 'multiple'=>false));
        if($response['ok']){
            echo sprintf("  更新OfferApi成功 \n");

            // 删除SSDB offer_api 缓存
            $ssdb->del('offer_apis_'.$thirdparty_offer_id.'_'.$network_id.'_'.$aff_id);
        }else{
            echo sprintf("  [error] 更新OfferApi失败 \n");
        }

    }else{
        echo sprintf("  不存在变化，不需要更新OfferApi \n");
    }
}


function update_offer($new_price, $new_cvn_cap, $thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb){
    // 判断是否已存在正式库中，如果有，则匹配更新字段。
    $offer = get_offer($thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb);
    // 只有当后台管理offer更新了 is_actived 和 is_locked ，才删除SSDB中的缓存
    if(!$offer){
        echo sprintf("  不存在于正式库中, 不需要同步更新 \n");
    }else{
        echo sprintf("  存在正式库中, 开始同步匹配更新字段 \n");

        if($offer['is_locked'] == 0){
            // 若存在offer_api库中，只判断api接口中的主要部分数据是否发生变化，再来更新offer_api
            $api_data = array(
                'price'      => $new_price,
                'cvn_cap'    => $new_cvn_cap,
                'is_actived' => 1,
            );
            $offer_data = array(
                'price'      => floatval($offer['price']),
                'cvn_cap'    => $offer['cvn_cap'],
                'is_actived' => $offer['is_actived'],
            );
            if(http_build_query($api_data) != http_build_query($offer_data)){
                echo sprintf("  存在变化，开始进行更新正式Offer \n");

                $where = array(
                    'thirdparty_offer_id' => $thirdparty_offer_id,
                    'network_id'          => $network_id,
                    'aff_id'              => $aff_id,
                );
                $effect = $mysql->update('t_offer', $api_data, $where);
                if($effect){
                    echo sprintf("  更新正式Offer成功 \n");

                    // 删除SSDB offer 缓存
                    $ssdb->hdel('offers', $thirdparty_offer_id.'_'.$network_id.'_'.$aff_id);
                }else{
                    echo sprintf("  [error] 更新正式Offer失败 \n");
                }
            }else{
                echo sprintf("  不存在变化，不需要更新正式Offer \n");
            }
        }else{
            echo sprintf("  锁住，跳过不更新 \n");
        }

    }

}

function update_offer2($new_price, $new_cvn_cap, $thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb){
    // 判断是否已存在正式库中，如果有，则匹配更新字段。
    $offer = get_offer2($thirdparty_offer_id, $network_id, $aff_id, $mysql, $ssdb);
    // 只有当后台管理offer更新了 is_actived 和 is_locked ，才删除SSDB中的缓存
    if(!$offer){
        echo sprintf("  不存在于正式库中, 不需要同步更新 \n");
    }else{
        echo sprintf("  存在正式库中, 开始同步匹配更新字段 \n");

        if($offer['is_locked'] == 0){
            // 若存在offer_api库中，只判断api接口中的主要部分数据是否发生变化，再来更新offer_api
            $api_data = array(
                'price'      => $new_price,
                'cvn_cap'    => $new_cvn_cap,
                'is_actived' => 1,
            );
            $offer_data = array(
                'price'      => floatval($offer['price']),
                'cvn_cap'    => $offer['cvn_cap'],
                'is_actived' => $offer['is_actived'],
            );
            if(http_build_query($api_data) != http_build_query($offer_data)){
                echo sprintf("  存在变化，开始进行更新正式Offer \n");

                $where = array(
                    'thirdparty_offer_id' => $thirdparty_offer_id,
                    'network_id'          => $network_id,
                    'aff_id'              => $aff_id,
                );
                $effect = $mysql->update('t_offer', $api_data, $where);
                if($effect){
                    echo sprintf("  更新正式Offer成功 \n");

                    // 删除SSDB offer 缓存
                    $ssdb->del('offers_'.$thirdparty_offer_id.'_'.$network_id.'_'.$aff_id);
                }else{
                    echo sprintf("  [error] 更新正式Offer失败 \n");
                }
            }else{
                echo sprintf("  不存在变化，不需要更新正式Offer \n");
            }
        }else{
            echo sprintf("  锁住，跳过不更新 \n");
        }

    }

}


/**
 * 没有返回，暂停临时库offer
 */
function pause_offer_api($api_offerid_list, $network_id, $aff_id, $mysql, $ssdb){
    if(!$api_offerid_list) return;

    $ids  = join(',', $api_offerid_list);
    $time = time();
    $sql = "select thirdparty_offer_id,network_id from `t_offer_api` where `thirdparty_offer_id` not in({$ids}) and network_id={$network_id} and aff_id='{$aff_id}'";
    $list = $mysql->executeSQL($sql);
    if(is_array($list)){
        if(isset($list['thirdparty_offer_id'])){
            $list = array($list);
        }
        
        $sql = "update `t_offer_api` set `is_actived`=0, `update_time`={$time} where `thirdparty_offer_id` not in({$ids}) and network_id={$network_id} and aff_id='{$aff_id}'";
        $effect = $mysql->executeSQL($sql);

        if($effect){
            foreach ($list as $key => $rs) {
                $ssdb->hdel('offer_apis', $rs['thirdparty_offer_id'].'_'.$rs['network_id'].'_'.$aff_id);
            }
        }
    }
}

function pause_offer_api2($api_offerid_list, $network_id, $aff_id, $db, $ssdb){
    if(!$api_offerid_list) return;
    $ids  = join(',', $api_offerid_list);
    

    // db.offer_apis.find({'network_id':78,'aff_id':'601','thirdparty_offer_id':{'$nin':['3235325','2860286']}})
    $where = array('$and' => array( array('network_id'=>$network_id), array('aff_id'=>$aff_id), array('is_actived'=>1), array('thirdparty_offer_id'=>array('$nin'=>$api_offerid_list)) ));
    $cursor = $db->offer_apis->find($where, array('thirdparty_offer_id'=>true, 'network_id'=>true));
    $list = iterator_to_array($cursor);
    if($list){

        $time = time();
        $update = array('$set' => array('is_actived' => 0, 'update_time' => $time));

        $response = $db->offer_apis->update($where, $update, array('upsert'=>true, 'multiple'=>true));
        if($response['ok']){
            echo sprintf("  OfferApi数据表里的暂停操作成功 \n");
            foreach ($list as $key => $rs) {
                $ssdb->del('offer_apis_'.$rs['thirdparty_offer_id'].'_'.$rs['network_id'].'_'.$aff_id);
            }
        }
    }
}


function pause_offer($api_offerid_list, $network_id, $aff_id, $mysql, $ssdb){
    if(!$api_offerid_list) return;

    $ids  = join(',', $api_offerid_list);
    // 1、获取本地已导入的offer id
    $sql = "select thirdparty_offer_id,network_id from t_offer where network_id={$network_id} and aff_id='{$aff_id}' and `thirdparty_offer_id` not in({$ids}) and is_deleted=0 and is_locked=0"; // 锁住的就不要管了，好吧
    $unreturn_list = $mysql->executeSQL($sql);

    if(is_array($unreturn_list)){
        if(isset($unreturn_list['thirdparty_offer_id'])){ // 兼容只存在1个对象，优化为列表
            $unreturn_list = array($unreturn_list);
        }

        $time = time();
        $sql = "update `t_offer` set `is_actived`=0, `update_time`={$time} where network_id={$network_id} and aff_id='{$aff_id}' and `thirdparty_offer_id` not in({$ids}) and is_deleted=0 and is_locked=0";
        $effect = $mysql->executeSQL($sql);

        if($effect){
            foreach ($unreturn_list as $key => $rs) {
                $ssdb->hdel('offers', $rs['thirdparty_offer_id'].'_'.$rs['network_id'].'_'.$aff_id);
            }
        }
    }
}

function pause_offer2($api_offerid_list, $network_id, $aff_id, $mysql, $ssdb){
    if(!$api_offerid_list) return;

    $ids  = join(',', $api_offerid_list);
    // 1、获取本地已导入的offer id
    $sql = "select thirdparty_offer_id,network_id from t_offer where network_id={$network_id} and aff_id='{$aff_id}' and `thirdparty_offer_id` not in({$ids}) and is_deleted=0 and is_locked=0 and is_actived=1"; // 锁住的就不要管了，好吧
    $unreturn_list = $mysql->executeSQL($sql);

    if(is_array($unreturn_list)){
        if(isset($unreturn_list['thirdparty_offer_id'])){ // 兼容只存在1个对象，优化为列表
            $unreturn_list = array($unreturn_list);
        }

        $time = time();
        $sql = "update `t_offer` set `is_actived`=0, `update_time`={$time} where network_id={$network_id} and aff_id='{$aff_id}' and `thirdparty_offer_id` not in({$ids}) and is_deleted=0 and is_locked=0 and is_actived=1";
        $effect = $mysql->executeSQL($sql);

        if($effect){
            foreach ($unreturn_list as $key => $rs) {
                $ssdb->del('offers_'.$rs['thirdparty_offer_id'].'_'.$rs['network_id'].'_'.$aff_id);
            }
        }
    }
}


?>