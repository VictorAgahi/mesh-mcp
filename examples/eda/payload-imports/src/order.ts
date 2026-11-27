// Decoy: same name as the alias the refund consumer imports `Refund` under.
export class Order {
  total: string;
}

// Decoy: same name as the type the package consumer imports from '@acme/contracts'.
export class Order2 {
  id: string;
  qty: number;
}

// Decoy: same name as the type alias declared in evalias.ts.
export class Evt {
  zz: boolean;
}
