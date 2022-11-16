package stock.ods;

import com.baidubce.http.ApiExplorerClient;
import com.baidubce.http.AppSigner;
import com.baidubce.http.HttpMethodName;
import com.baidubce.model.ApiExplorerRequest;
import com.baidubce.model.ApiExplorerResponse;
import org.apache.calcite.util.Static;

public class GetStockData {

    private final static String accesskey = "";
    private final static String secretkey = "";
    private final static String code = "600004";

    public static String getStockResponse(ApiExplorerRequest request ){
        ApiExplorerClient client = new ApiExplorerClient(new AppSigner());

        try {
            ApiExplorerResponse response = client.sendRequest(request);
            // 返回结果格式为Json字符串
            return response.getResult();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return " ";
    }

    //基于股指实时分时线图
    public static String getStockTimeLine(){
        String path = "http://stocks.api.bdymkt.com/stockTimeLine";
        ApiExplorerRequest request = new ApiExplorerRequest(HttpMethodName.GET, path);
        request.setCredentials(accesskey, secretkey);

        request.addHeaderParameter("Content-Type", "application/json;charset=UTF-8");

        request.addQueryParameter("code", code);
        request.addQueryParameter("day", "2");

        return getStockResponse(request);
    }



    //股票最近50条交易数据
    public static String getStock50(){
        String path = "http://stocks.api.bdymkt.com/stock50";
        ApiExplorerRequest request = new ApiExplorerRequest(HttpMethodName.GET, path);
        request.setCredentials(accesskey, secretkey);

        request.addHeaderParameter("Content-Type", "application/json;charset=UTF-8");

        request.addQueryParameter("code", code);

        return getStockResponse(request);
    }

    //实时批量查询
    public static String getStockRealTimeBatch(){
        String path = "http://stocks.api.bdymkt.com/stockRealTimeBatch";
        ApiExplorerRequest request = new ApiExplorerRequest(HttpMethodName.GET, path);
        request.setCredentials(accesskey, secretkey);

        request.addHeaderParameter("Content-Type", "application/json;charset=UTF-8");

        request.addQueryParameter("stocks", code);
        request.addQueryParameter("needIndex", "1");

        return getStockResponse(request);
    }


    //历史日线数据
    public static String getStockHistoryK(){
        // 股票历史日线 Java示例代码
        String path = "http://stocks.api.bdymkt.com/stockHistoryK";
        ApiExplorerRequest request = new ApiExplorerRequest(HttpMethodName.GET, path);
        request.setCredentials(accesskey, secretkey);

        request.addHeaderParameter("Content-Type", "application/json;charset=UTF-8");

        request.addQueryParameter("begin", "2016-09-01");
        request.addQueryParameter("code", code);
        request.addQueryParameter("end", "2016-09-12");
        request.addQueryParameter("type", "bfq");

        return getStockResponse(request);
    }

}


