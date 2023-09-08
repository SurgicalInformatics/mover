import torch
from torch import nn


class LSTM(nn.Module):
    def __init__(self, input_dim, hidden_dim, act_layer=nn.GELU, drop=0.0, **kwargs):
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim

        self.proj = nn.Linear(input_dim, hidden_dim)
        self.act = act_layer()
        self.lstm = nn.LSTM(input_size=input_dim, hidden_size=hidden_dim, num_layers=1, batch_first=True)
        self.output = nn.Linear(hidden_dim, 1)

    def forward(self, x, mask):
        x, _ = self.lstm(x)
        last_hidden = x[torch.arange(x.shape[0]), mask.sum(dim=1) - 1]
        out = self.output(last_hidden)
        return torch.sigmoid(out)
    
    def get_loss(self, x, y, mask):
        out = self.forward(x, mask)
        loss = nn.BCELoss()(out, y)
        return loss