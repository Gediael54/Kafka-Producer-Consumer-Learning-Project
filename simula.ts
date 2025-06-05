import { Kafka, Producer, Consumer } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';

interface Ticket {
  id: string;
  name: string;
  email: string;
  quantity: number;
  price: number;
  createdAt: string;
}

interface ProcessedTicket {
  id: string;
  status: 'CONFIRMED' | 'FAILED' | 'PENDING';
  errorMessage?: string;
  processedAt: string;
}

class TicketPurchaseProducer {
  private kafka: Kafka;
  private producer: Producer;

  constructor() {
    this.kafka = new Kafka({
      clientId: 'ticket-purchase-producer',
      brokers: ['localhost:9092']
    });
    this.producer = this.kafka.producer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
    console.log('Producer de ingressos conectado');
  }

  async simulatePurchase(customerData: any): Promise<boolean> {
    const purchase: Ticket = {
      id: uuidv4(),
      name: customerData.name,
      email: customerData.email,
      quantity: customerData.quantity,
      price: 10,
      createdAt: new Date().toISOString(),
    };

    try {
      await this.producer.send({
        topic: 'ticket-purchases',
        messages: [{
          key: purchase.id,
          value: JSON.stringify(purchase),
          headers: {
            eventType: 'TICKET_PURCHASE'
          }
        }]
      });

      console.log(`Compra enviada para processamento: ${purchase.id}`);
      console.log(`- Cliente: ${purchase.name}`);
      console.log(`- Ingressos: ${purchase.quantity} x R$ ${purchase.price}`);
      return true;

    } catch (error) {
      console.error('Erro ao enviar compra:', error);
      return false;
    }
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
    console.log('Producer desconectado');
  }
}

class TicketProcessingConsumer {
  private kafka: Kafka;
  private consumer: Consumer;
  private availableTickets: number = 10;

  constructor() {
    this.kafka = new Kafka({
      clientId: 'ticket-processing-consumer',
      brokers: ['localhost:9092']
    });
    this.consumer = this.kafka.consumer({ 
      groupId: 'ticket-processing-group' 
    });
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ 
      topic: 'ticket-purchases', 
      fromBeginning: true 
    });
    console.log('Consumer de processamento conectado');
  }

  async startProcessing(): Promise<void> {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const purchaseData: Ticket = JSON.parse(message.value!.toString());
          await this.processPurchase(purchaseData);
        } catch (error) {
          console.error('Erro ao processar mensagem:', error);
        }
      }
    });
  }

  private async processPurchase(purchase: Ticket): Promise<void> {
    console.log(`\nProcessando compra: ${purchase.id}`);
    await this.sleep(1000);

    if (this.availableTickets < purchase.quantity) {
      await this.handleFailedPurchase(purchase, 'Ingressos esgotados');
      return;
    }

    await this.handleSuccessfulPurchase(purchase);
  }

  private async handleSuccessfulPurchase(purchase: Ticket): Promise<void> {
    this.availableTickets -= purchase.quantity;
    const processedTicket: ProcessedTicket = {
      id: purchase.id,
      status: 'CONFIRMED',
      processedAt: new Date().toISOString()
    };

    console.log(`COMPRA CONFIRMADA!`);
    console.log(`- Cliente: ${purchase.name}`);
    console.log(`- Email: ${purchase.email}`);
    console.log(`- Restam: ${this.availableTickets} ingressos disponíveis`);
    console.log(processedTicket);
  }

  private async handleFailedPurchase(purchase: Ticket, reason: string): Promise<void> {
    const processedTicket: ProcessedTicket = {
      id: purchase.id,
      status: 'FAILED',
      errorMessage: reason,
      processedAt: new Date().toISOString()
    };

    console.log(`Erro na compra: ${purchase.id}`);
    console.log(`- Motivo: ${reason}`);
    console.log(`- Cliente: ${purchase.name}`);
    console.log(processedTicket);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  getAvailableTickets(): number {
    return this.availableTickets;
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    console.log('Consumer desconectado');
  }
}

class TicketPurchaseSimulator {
  private producer: TicketPurchaseProducer;

  constructor() {
    this.producer = new TicketPurchaseProducer();
  }

  async init(): Promise<void> {
    await this.producer.connect();
  }

  async simulateRandomPurchases(count: number = 5): Promise<void> {
    const customers = [
      { name: 'João Silva', email: 'joao@email.com' },
      { name: 'Maria Santos', email: 'maria@email.com' },
      { name: 'Pedro Oliveira', email: 'pedro@email.com' },
      { name: 'Ana Costa', email: 'ana@email.com' },
      { name: 'Carlos Lima', email: 'carlos@email.com' }
    ];

    for (let i = 0; i < count; i++) {
      const customer = customers[Math.floor(Math.random() * customers.length)];
      const quantity = Math.floor(Math.random() * 3) + 1;
      const purchaseData = {
        ...customer,
        quantity
      };

      await this.producer.simulatePurchase(purchaseData);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  async cleanup(): Promise<void> {
    await this.producer.disconnect();
  }
}

async function exemploCompleto(): Promise<void> {
  console.log('SISTEMA DE INGRESSOS COM KAFKA');
  console.log('='.repeat(40));
  
  console.log('\nIngressos disponíveis: 10');
  console.log('Preço: R$ 10,00 cada');

  const consumer = new TicketProcessingConsumer();
  await consumer.connect();
  consumer.startProcessing().catch(console.error);

  const simulator = new TicketPurchaseSimulator();
  await simulator.init();
  
  console.log('\nIniciando simulação de compras...\n');
  await simulator.simulateRandomPurchases(8);
  
  console.log('\nAguardando processamento finalizar...');
  await new Promise(resolve => setTimeout(resolve, 10000));
  
  await simulator.cleanup();
  await consumer.disconnect();
  
  console.log('\nSimulação finalizada!');
}

if (require.main === module) {
  exemploCompleto().catch(console.error);
}

export { 
  TicketPurchaseProducer, 
  TicketProcessingConsumer, 
  TicketPurchaseSimulator,
  Ticket,
  ProcessedTicket 
};