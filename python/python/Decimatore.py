import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import decimate
from scipy.fft import fft, fftfreq

# Parametri segnale
Fs = 48000            # Frequenza di campionamento originale in Hz
M = 8                 # Fattore di decimazione
Fs_dec = Fs // M      # Nuova frequenza di campionamento

# Durata e tempo
T = 0.05              # Durata segnale in secondi
t = np.arange(0, T, 1/Fs)

# Signal: componente utile a 4 kHz + componente problematicamente alta a 10 kHz
x = np.sin(2*np.pi*4000*t) + 0.5*np.sin(2*np.pi*10000*t)

# Decimazione con filtro anti-aliasing
y = decimate(x, M, ftype='fir', zero_phase=True)

# Tempo dopo decimazione
t_dec = np.arange(0, T, 1/Fs_dec)

# Funzione per calcolare e plot dello spettro
def plot_spectrum(signal, Fs, ax, title):
    N = len(signal)
    Xf = fft(signal)
    freqs = fftfreq(N, d=1/Fs)
    idx = np.argsort(freqs)
    ax.plot(freqs[idx], 20*np.log10(np.abs(Xf)[idx] + 1e-12))
    ax.set_xlim(0, Fs/2)
    ax.set_xlabel('Freq (Hz)')
    ax.set_ylabel('Magnitude (dB)')
    ax.set_title(title)

# Visualizza risultati
fig, axs = plt.subplots(2, 2, figsize=(12, 8))

# Dominio del tempo
axs[0, 0].plot(t, x, label='Originale')
axs[0, 0].set_title('Segnale originale (tempo)')
axs[0, 0].set_xlabel('Tempo (s)')
axs[0, 0].set_ylabel('Ampiezza')
axs[0, 0].legend()

axs[0, 1].plot(t_dec, y, label='Decimato', color='orange')
axs[0, 1].set_title(f'Segnale decimato (tempo), Fs\'={Fs_dec} Hz')
axs[0, 1].set_xlabel('Tempo (s)')
axs[0, 1].legend()

# Spettro
plot_spectrum(x, Fs, axs[1, 0], 'Spettro originale')
plot_spectrum(y, Fs_dec, axs[1, 1], 'Spettro decimato')

plt.tight_layout()
plt.show()
