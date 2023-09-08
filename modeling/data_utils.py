import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader

class EHRDataset(Dataset):
    def __init__(self, data_dict):
        self.data_dict = data_dict
        self.keys = list(data_dict.keys())

    def __len__(self):
        return len(self.keys)

    def __getitem__(self, idx):
        return self.data_dict[self.keys[idx]][0], self.data_dict[self.keys[idx]][1]
    
def collate_fn(batch):
    """
    Args:
        batch: list of tuples of (x, y)    
    
    Returns:
        x: list of torch tensors
        y: list of torch tensors
        mask: list of torch tensors
    """
    max_len = max([len(x[0]) for x in batch])
    feat_dim = batch[0][0].shape[1]
    x = torch.zeros((len(batch), max_len, feat_dim), dtype=torch.float32)
    y = torch.zeros((len(batch)), dtype=torch.float32)
    mask = torch.zeros((len(batch), max_len), dtype=torch.long)
    for i, (x_i, y_i) in enumerate(batch):
        x[i, :len(x_i)] = torch.tensor(x_i)
        y[i] = y_i
        mask[i, :len(x_i)] = 1
    return x, y, mask

def get_dataloader(data_dict, batch_size=32, shuffle=True, num_workers=0):
    dataset = EHRDataset(data_dict)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers, collate_fn=collate_fn)
    return dataloader