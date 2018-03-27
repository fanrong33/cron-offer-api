<?php
/**
 * PHP Library for Appcoach
 * @version 1.0.2 build 20180307
 */
class Appcoach{

    function __construct($api_key){
        $this->api_key = $api_key;
    }

    /**
     * 获取offer列表
     * @param  array  $params
     * array(
     *     'page_index' => 1,
     *     'page_size'  => 100
     * )
     */
    function get_offer($params) {
        $url = 'https://api.affiliate.appcoachs.com/v1/getoffers';

        return $this->api($url, $params);
    }

    function api($url, $params, $method='GET'){
        $params['api_key'] = $this->api_key;

        if($method == 'GET'){
            $result_str = $this->http($url.'?'.http_build_query($params));
        }else{
            $result_str = $this->http($url, http_build_query($params), 'POST');
        }
        $result = array();
        if($result_str!='') $result = json_decode($result_str, true);
        return $result;
    }

    function http($url, $postfields='', $method='GET', $headers=array()){
        $ci = curl_init();
        curl_setopt($ci, CURLOPT_SSL_VERIFYPEER, FALSE); 
        curl_setopt($ci, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ci, CURLOPT_CONNECTTIMEOUT, 60);
        curl_setopt($ci, CURLOPT_TIMEOUT, 60);
        if($method=='POST'){
            curl_setopt($ci, CURLOPT_POST, TRUE);
            if($postfields!='')curl_setopt($ci, CURLOPT_POSTFIELDS, $postfields);
        }
        // $headers[]="User-Agent: ";
        curl_setopt($ci, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ci, CURLOPT_URL, $url);
        $response = curl_exec($ci);
        curl_close($ci);
        return $response;
    }

}

?>