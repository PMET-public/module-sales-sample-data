<?php
/**
 * Copyright © 2016 Magento. All rights reserved.
 * See COPYING.txt for license details.
 */
namespace MagentoEse\SalesSampleData\Model;

use Magento\Framework\Setup\SampleData\Context as SampleDataContext;

/**
 * Class Order
 */
class Order
{
    /**
     * @var \Magento\Framework\File\Csv
     */
    protected $csvReader;

    /**
     * @var \Magento\Framework\Setup\SampleData\FixtureManager
     */
    protected $fixtureManager;

    /**
     * @var Order\Converter
     */
    protected $converter;

    /**
     * @var Order\Processor
     */
    protected $orderProcessor;

    /**
     * @var \Magento\Sales\Model\ResourceModel\Order\CollectionFactory
     */
    protected $orderCollectionFactory;

    /**
     * @var \Magento\Customer\Api\CustomerRepositoryInterface
     */
    protected $customerRepository;

    protected $updateSalesData;
    protected $resourceConnection;

    public function __construct(
        SampleDataContext $sampleDataContext,
        Order\Converter $converter,
        Order\Processor $orderProcessor,
        \Magento\Sales\Model\ResourceModel\Order\CollectionFactory $orderCollectionFactory,
        \Magento\Customer\Api\CustomerRepositoryInterface $customerRepository,
        \Magento\Sales\Model\OrderFactory $orderFactory,
        \MagentoEse\SalesSampleData\Cron\UpdateSalesData $updateSalesData,
        \Magento\Framework\App\ResourceConnection $resourceConnection
    ) {
        $this->fixtureManager = $sampleDataContext->getFixtureManager();
        $this->csvReader = $sampleDataContext->getCsvReader();
        $this->converter = $converter;
        $this->orderProcessor = $orderProcessor;
        $this->orderCollectionFactory = $orderCollectionFactory;
        $this->customerRepository = $customerRepository;
        $this->orderFactory = $orderFactory;
        $this->updateSalesData = $updateSalesData;
        $this->resourceConnection = $resourceConnection;
    }

    /**
     * {@inheritdoc}
     */
    public function install(array $fixtures,$shiftDates = false)
    {
        $shiftHours = 100000000;
        foreach ($fixtures as $file) {
            $fileName = $this->fixtureManager->getFixture($file);
            if (!file_exists($fileName)) {
                throw new Exception('File not found: '.$fileName);
            }

            $rows = $this->csvReader->getData($fileName);
            $header = array_shift($rows);
            $orders = [];
            foreach ($rows as $row) {

                $data = [];
                foreach ($row as $key => $value) {
                    $data[$header[$key]] = $value;
                }
                $row = $data;
                $orderData = $this->converter->convertRow($row);
                $orderId = $this->orderProcessor->createOrder($orderData);
                if($shiftDates){
                    ///find the latest date
                    $createdAt = date_create($data['created_at']);
                    $currDate = date_create(date("Y-m-d h:i:sa"));
                    $diff = date_diff($createdAt,$currDate );
                    $hours = ($diff->y * 365.25 + $diff->m * 30 + $diff->d) * 24 + $diff->h;
                    if($hours < $shiftHours){
                        $shiftHours = $hours;
                    }
                    $orders[]=$orderId;
                }
            }
            if($shiftDates){
                $this->updateOrderDates($orders,$shiftHours);
            }
            unset($data);
            unset($orders);

        }
        $this->updateSalesData->refreshStatistics();

    }
    private function updateOrderDates(array $orders, $shiftHours){
        foreach($orders as $orderId){
            $this->updateOrderData($orderId,$shiftHours);
            $this->updateInvoiceData($orderId,$shiftHours);
            $this->updateShipmentData($orderId,$shiftHours);
        }

    }
    private function updateOrderData($orderId,$dateDiff){
        //sales_order,sales_order_grid
        $connection = $this->resourceConnection->getConnection();
        $tableName = $connection->getTableName('sales_order');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR) where entity_id=".$orderId;
        $connection->query($sql);
        $tableName = $connection->getTableName('sales_order_grid');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR) where entity_id=".$orderId;
        $connection->query($sql);
        $tableName = $connection->getTableName('sales_order_item');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR) where order_id=".$orderId;
        $connection->query($sql);

    }

    private function updateInvoiceData($orderId,$dateDiff){
        $connection = $this->resourceConnection->getConnection();
        $tableName = $connection->getTableName('sales_invoice');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR) where order_id=".$orderId;
        $connection->query($sql);
        $tableName = $connection->getTableName('sales_invoice_grid');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR), order_created_at =  DATE_ADD(order_created_at,INTERVAL ".$dateDiff." HOUR) where order_id=".$orderId;
        $connection->query($sql);

    }

    private function updateShipmentData($orderId,$dateDiff){
        $connection = $this->resourceConnection->getConnection();
        $tableName = $connection->getTableName('sales_shipment');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR) where order_id=".$orderId;
        $connection->query($sql);
        $tableName = $connection->getTableName('sales_shipment_grid');
        $sql = "update " . $tableName . " set created_at =  DATE_ADD(created_at,INTERVAL ".$dateDiff." HOUR), updated_at =  DATE_ADD(updated_at,INTERVAL ".$dateDiff." HOUR), order_created_at =  DATE_ADD(order_created_at,INTERVAL ".$dateDiff." HOUR) where order_id=".$orderId;
        $connection->query($sql);

    }
}
