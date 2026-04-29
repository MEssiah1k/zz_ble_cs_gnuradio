clc
% clear all
close all

fs=2e7;

fc=2.409e9;
signal_fc=2*pi*fc/fs;

fi_1 = fopen('/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_initiator_rx_from_reflector_2m','rb');
x_inter_1 = fread(fi_1, 'float32');

% if data is complex
x_1 = x_inter_1(1:2:end) + 1i*x_inter_1(2:2:end);

% x_1 = x.*exp(i.*phase);


% 绘制幅值和相位图
figure;

% 绘制幅值
subplot(2, 1, 1);
plot(abs(x_1));
title('信号幅值');
xlabel('样本点');
ylabel('幅值');
grid on;

% 绘制相位
subplot(2, 1, 2);
plot(angle(x_1));
title('信号相位');
xlabel('样本点');
ylabel('相位 (弧度)');
grid on;

fi_2 = fopen('/home/lfy/workarea/zz_ble_cs_gnuradio/1to1_2sides/data_reflector_rx_from_initiator_2m','rb');
x_inter_2 = fread(fi_2, 'float32');

% if data is complex
x_2 = x_inter_2(1:2:end) + 1i*x_inter_2(2:2:end);

% 绘制幅值和相位图
figure;

% 绘制幅值
subplot(2, 1, 1);
plot(abs(x_2));
title('信号幅值');
xlabel('样本点');
ylabel('幅值');
grid on;

% 绘制相位
subplot(2, 1, 2);
plot(angle(x_2));
title('信号相位');
xlabel('样本点');
ylabel('相位 (弧度)');
grid on;
len = round(size(x_1,1)/26);
y = (1:len)/2e7;
phase = (0.8e6:1.5e6+5)*signal_fc;
plot(0.8e6:1.5e6,abs(x_1(0.8e6:1.5e6)));

x_1 = x_1(0.8e6:1.5e6+5).*exp(i*phase');
data = x_1(1:end-5);
data_de = x_1(6:end);

f_data = fft(data);
figure();
plot(abs(f_data));

% e_data = exp(i*angle(data));
% e_data_de = exp(i*angle(data_de));


data_add = data + data_de;
f_data_add = fft(data_add);
figure();
plot(abs(f_data_add));

% (sig_A + sig_B).*(sig_A + sig_B);

% data_com = real(data).*real(data_de);
data_com = (data + data_de).*(data + data_de);
f_data_com = fft(data_com);
figure();
plot(abs(f_data_com));

% N=8;
% [b,a]=butter(N,40e5/(fs/2),"low");
fs = 2e7;           % 采样率 20MHz
f_low = 1e5;        % 下截止频率 1MHz
f_high = 1e6;       % 上截止频率 3MHz

% 计算归一化截止频率 [Wn1, Wn2]
% 范围必须在 0 到 1 之间，1 代表 fs/2
Wn = [f_low, f_high] / (fs/2); 

% 设计 4 阶带通滤波器 (注意：实际生成的系统阶数会翻倍)
N = 4; 
[b, a] = butter(N, Wn, 'bandpass');

data_com_f = filter(b,a,data_com);

f_data_com_f = fft(data_com_f);
figure();
plot(abs(f_data_com));
% 
figure();
% plot(abs(data_com));


hold on;
plot(abs(data_com_f));

