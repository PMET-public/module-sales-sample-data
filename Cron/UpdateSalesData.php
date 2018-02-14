<?php

namespace MagentoEse\SalesSampleData\Cron;


class UpdateSalesData
{
    protected $logger;
    protected $resourceConnection;
    protected $resourceModel;
    protected $aggregateSalesReportBestsellersData;
    protected $aggregateSalesReportInvoicedData;
    protected $aggregateSalesReportOrderData;

    public function __construct(
        \Psr\Log\LoggerInterface $logger,
        \Magento\Framework\App\ResourceConnection $resourceConnection,
        \Magento\Sales\Model\CronJob\AggregateSalesReportBestsellersData $aggregateSalesReportBestsellersData,
        \Magento\Sales\Model\CronJob\AggregateSalesReportInvoicedData $aggregateSalesReportInvoicedData,
        \Magento\Sales\Model\CronJob\AggregateSalesReportOrderData $aggregateSalesReportOrderData
    ) {
        $this->logger = $logger;
        $this->resourceConnection = $resourceConnection;
        $this->aggregateSalesReportBestsellersData = $aggregateSalesReportBestsellersData;
        $this->aggregateSalesReportInvoicedData = $aggregateSalesReportInvoicedData;
        $this->aggregateSalesReportOrderData = $aggregateSalesReportOrderData;
    }

    /**
     * Method executed when cron runs in server
     */
    public function execute($dayShift) {
        if(!is_int($dayShift)) {
            $dayShift = $this->getDateDiff();
        }
        $this->updateOrderData($dayShift);
        $this->updateInvoiceData($dayShift);
        $this->updateShipmentData($dayShift);
        $this->updateCustomerData($dayShift);
        $this->refreshStatistics();
        $this->logger->debug('Ran Sales update data');
        return $this;
    }

    private function updateOrderData($dateDiff){
        //sales_order,sales_order_grid
        $connection = $this->resourceConnection->getConnection();
        $tableName = $connection->getTableName('sales_order');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR)";
        $connection->query($sql);
        $tableName = $connection->getTableName('sales_order_grid');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR)";
        $connection->query($sql);
        $orderItemTableName = $connection->getTableName('sales_order_item');
        $orderTableName = $connection->getTableName('sales_order');
        $sql = "update " . $orderTableName . " so, ".$orderItemTableName." oi set oi.created_at = so.created_at, oi.updated_at = so.updated_at where oi.order_id = so.entity_id";
        $connection->query($sql);
    }

    private function updateInvoiceData($dateDiff){
        $connection = $this->resourceConnection->getConnection();
        $tableName = $connection->getTableName('sales_invoice');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR)";
        $connection->query($sql);
        $tableName = $connection->getTableName('sales_invoice_grid');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR), order_created_at =  DATE_ADD(order_created_at,INTERVAL ".$dateDiff." HOUR)";
        $connection->query($sql);

    }

    private function updateShipmentData($dateDiff){
        $connection = $this->resourceConnection->getConnection();
        $tableName = $connection->getTableName('sales_shipment');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR)";
        $connection->query($sql);
        $tableName = $connection->getTableName('sales_shipment_grid');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR), order_created_at =  DATE_ADD(order_created_at,INTERVAL ".$dateDiff." HOUR)";
        $connection->query($sql);

    }

    private function updateCustomerData($dateDiff){
        //set user create dates
        $connection = $this->resourceConnection->getConnection();
        $customerTableName = $connection->getTableName('customer_entity');
        $sql = "select DATEDIFF(now(), max(created_at)) * 24 + EXTRACT(HOUR FROM now()) - EXTRACT(HOUR FROM max(created_at)) -1 as hours from ".$customerTableName;
        $result = $connection->fetchAll($sql);
        $dateDiff =  $result[0]['hours']+$dateDiff;
        $sql = "update ".$customerTableName." set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR)";
        $connection->query($sql);

    }

    public function refreshStatistics(){
        $this->aggregateSalesReportOrderData->execute();
        $this->aggregateSalesReportBestsellersData->execute();
        $this->aggregateSalesReportInvoicedData->execute();

    }

    private function getDateDiff(){
        $connection = $this->resourceConnection->getConnection();
        $tableName = $connection->getTableName('sales_order');
        $sql = "select DATEDIFF(now(), max(created_at)) * 24 + EXTRACT(HOUR FROM now()) - EXTRACT(HOUR FROM max(created_at)) -1 as hours from " . $tableName;
        $result = $connection->fetchAll($sql);
        return $result[0]['hours'];
    }


}