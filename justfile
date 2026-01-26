# Sync TypeScript bindings to the Node repo
sync-bindings:
    cargo test export_bindings --release -- --nocapture
    @echo "✅ Bindings synced to modern-product-repo"
