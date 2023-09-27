from torch import nn, zeros
from config import TARGETS, FORECAST_HORIZON


class GRU(nn.Module):
    def __init__(
            self,
            n_input_ts: int = len(TARGETS),
            hidden_size: int = 100,
            num_layers: int = 1,
            dropout: float = 0.0,
            n_output_ts: int = len(TARGETS),
            horizon: int = FORECAST_HORIZON
    ):

        super().__init__()

        # number of time-series in the input
        self.n_input_ts = n_input_ts

        # number of neurons in the hidden layer
        self.hidden_size = hidden_size

        # number of layers in the network
        self.num_layers = num_layers

        # dropout (only used in case of multiple rnn-layers)
        self.dropout = dropout if self.num_layers > 1 else 0

        # number of time-series to predict
        self.n_output_ts = n_output_ts

        # number of future steps to predict
        self.horizon = horizon

        # RNN-layers
        self.gru = nn.GRU(input_size=self.n_input_ts,
                          hidden_size=self.hidden_size,
                          num_layers=self.num_layers,
                          dropout=self.dropout,
                          batch_first=True)

        # a linear output layer
        self.output_layer = nn.Linear(
            in_features=self.hidden_size,
            out_features=self.n_output_ts * horizon
        )

    def forward(self, x, padding_masks=None):   # noqa. Accept parameter padding masks for compatibility

        # Initializing hidden state for first input with zeros
        h0 = zeros(self.num_layers, x.size(0), self.hidden_size).requires_grad_().to(x.device)

        # shape of x: (batch_size, seq_len, n_input_ts)
        # shape of gru_out: (batch_size, seq_len, hidden_size)
        gru_out, _ = self.gru(x, h0.detach())

        # shape of output: (batch_size, seq_len, horizon * n_output_ts)
        output = self.output_layer(gru_out)

        # re-shape to shape (batch_size, seq_len, horizon, n_output_ts)
        output = output.reshape(output.size(0), output.size(1), self.horizon, self.n_output_ts)

        return output
