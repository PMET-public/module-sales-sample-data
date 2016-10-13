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

    public function __construct(
        SampleDataContext $sampleDataContext,
        Order\Converter $converter,
        Order\Processor $orderProcessor,
        \Magento\Sales\Model\ResourceModel\Order\CollectionFactory $orderCollectionFactory,
        \Magento\Customer\Api\CustomerRepositoryInterface $customerRepository,
        \Magento\Sales\Model\OrderFactory $orderFactory,
        \MagentoEse\SalesSampleData\Cron\UpdateSalesData $updateSalesData
    ) {
        $this->fixtureManager = $sampleDataContext->getFixtureManager();
        $this->csvReader = $sampleDataContext->getCsvReader();
        $this->converter = $converter;
        $this->orderProcessor = $orderProcessor;
        $this->orderCollectionFactory = $orderCollectionFactory;
        $this->customerRepository = $customerRepository;
        $this->orderFactory = $orderFactory;
        $this->updateSalesData = $updateSalesData;
    }

    /**
     * {@inheritdoc}
     */
    public function install(array $fixtures)
    {

        foreach ($fixtures as $file) {
            $fileName = $this->fixtureManager->getFixture($file);
            if (!file_exists($fileName)) {
                continue;
            }

            $rows = $this->csvReader->getData($fileName);
            $header = array_shift($rows);

            foreach ($rows as $row) {
                $data = [];
                foreach ($row as $key => $value) {
                    $data[$header[$key]] = $value;
                }
                $row = $data;
                $orderData = $this->converter->convertRow($row);
                $this->orderProcessor->createOrder($orderData);
                //update order dates
                $this->updateSalesData->execute();
            }
        }
    }
}
