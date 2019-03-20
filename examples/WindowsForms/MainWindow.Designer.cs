namespace WindowsForms
{
    partial class MainWindow
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.produceAsyncButton = new System.Windows.Forms.Button();
            this.beginProduceButton = new System.Windows.Forms.Button();
            this.consumeButton = new System.Windows.Forms.Button();
            this.resultListBox = new System.Windows.Forms.ListBox();
            this.bootstrapServersLabel = new System.Windows.Forms.Label();
            this.schemaRegistryUrlLabel = new System.Windows.Forms.Label();
            this.bootstrapServersTextBox = new System.Windows.Forms.TextBox();
            this.schemaRegistryUrlTextBox = new System.Windows.Forms.TextBox();
            this.connectButton = new System.Windows.Forms.Button();
            this.topicNameTextBox = new System.Windows.Forms.TextBox();
            this.topicNameLabel = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // produceAsyncButton
            // 
            this.produceAsyncButton.Location = new System.Drawing.Point(43, 379);
            this.produceAsyncButton.Name = "produceAsyncButton";
            this.produceAsyncButton.Size = new System.Drawing.Size(318, 97);
            this.produceAsyncButton.TabIndex = 0;
            this.produceAsyncButton.Text = "ProduceAsync";
            this.produceAsyncButton.UseVisualStyleBackColor = true;
            this.produceAsyncButton.Click += new System.EventHandler(this.produceAsyncButton_Click);
            // 
            // beginProduceButton
            // 
            this.beginProduceButton.Location = new System.Drawing.Point(410, 379);
            this.beginProduceButton.Name = "beginProduceButton";
            this.beginProduceButton.Size = new System.Drawing.Size(318, 97);
            this.beginProduceButton.TabIndex = 1;
            this.beginProduceButton.Text = "BeginProduce";
            this.beginProduceButton.UseVisualStyleBackColor = true;
            this.beginProduceButton.Click += new System.EventHandler(this.beginProduceButton_Click);
            // 
            // consumeButton
            // 
            this.consumeButton.Location = new System.Drawing.Point(771, 379);
            this.consumeButton.Name = "consumeButton";
            this.consumeButton.Size = new System.Drawing.Size(318, 97);
            this.consumeButton.TabIndex = 2;
            this.consumeButton.Text = "Consume";
            this.consumeButton.UseVisualStyleBackColor = true;
            this.consumeButton.Click += new System.EventHandler(this.consumeButton_Click);
            // 
            // resultListBox
            // 
            this.resultListBox.FormattingEnabled = true;
            this.resultListBox.ItemHeight = 25;
            this.resultListBox.Location = new System.Drawing.Point(43, 507);
            this.resultListBox.Name = "resultListBox";
            this.resultListBox.Size = new System.Drawing.Size(1046, 279);
            this.resultListBox.TabIndex = 3;
            // 
            // bootstrapServersLabel
            // 
            this.bootstrapServersLabel.AutoSize = true;
            this.bootstrapServersLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.bootstrapServersLabel.Location = new System.Drawing.Point(36, 42);
            this.bootstrapServersLabel.Name = "bootstrapServersLabel";
            this.bootstrapServersLabel.Size = new System.Drawing.Size(264, 37);
            this.bootstrapServersLabel.TabIndex = 4;
            this.bootstrapServersLabel.Text = "bootstrap.servers";
            // 
            // schemaRegistryUrlLabel
            // 
            this.schemaRegistryUrlLabel.AutoSize = true;
            this.schemaRegistryUrlLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.schemaRegistryUrlLabel.Location = new System.Drawing.Point(36, 109);
            this.schemaRegistryUrlLabel.Name = "schemaRegistryUrlLabel";
            this.schemaRegistryUrlLabel.Size = new System.Drawing.Size(287, 37);
            this.schemaRegistryUrlLabel.TabIndex = 5;
            this.schemaRegistryUrlLabel.Text = "schema.registry.url";
            // 
            // bootstrapServersTextBox
            // 
            this.bootstrapServersTextBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.bootstrapServersTextBox.Location = new System.Drawing.Point(349, 35);
            this.bootstrapServersTextBox.Name = "bootstrapServersTextBox";
            this.bootstrapServersTextBox.Size = new System.Drawing.Size(738, 44);
            this.bootstrapServersTextBox.TabIndex = 6;
            this.bootstrapServersTextBox.Text = "127.0.0.1:9092";
            // 
            // schemaRegistryUrlTextBox
            // 
            this.schemaRegistryUrlTextBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.schemaRegistryUrlTextBox.Location = new System.Drawing.Point(349, 103);
            this.schemaRegistryUrlTextBox.Name = "schemaRegistryUrlTextBox";
            this.schemaRegistryUrlTextBox.Size = new System.Drawing.Size(738, 44);
            this.schemaRegistryUrlTextBox.TabIndex = 7;
            this.schemaRegistryUrlTextBox.Text = "127.0.0.1:8081";
            // 
            // connectButton
            // 
            this.connectButton.Location = new System.Drawing.Point(843, 234);
            this.connectButton.Name = "connectButton";
            this.connectButton.Size = new System.Drawing.Size(244, 56);
            this.connectButton.TabIndex = 8;
            this.connectButton.Text = "Connect";
            this.connectButton.UseVisualStyleBackColor = true;
            this.connectButton.Click += new System.EventHandler(this.connectButton_Click);
            // 
            // topicNameTextBox
            // 
            this.topicNameTextBox.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.topicNameTextBox.Location = new System.Drawing.Point(349, 171);
            this.topicNameTextBox.Name = "topicNameTextBox";
            this.topicNameTextBox.Size = new System.Drawing.Size(738, 44);
            this.topicNameTextBox.TabIndex = 9;
            this.topicNameTextBox.Text = "windows-forms-example";
            // 
            // topicNameLabel
            // 
            this.topicNameLabel.AutoSize = true;
            this.topicNameLabel.Font = new System.Drawing.Font("Microsoft Sans Serif", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.topicNameLabel.Location = new System.Drawing.Point(36, 174);
            this.topicNameLabel.Name = "topicNameLabel";
            this.topicNameLabel.Size = new System.Drawing.Size(85, 37);
            this.topicNameLabel.TabIndex = 10;
            this.topicNameLabel.Text = "topic";
            // 
            // MainWindow
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(12F, 25F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(1127, 819);
            this.Controls.Add(this.topicNameLabel);
            this.Controls.Add(this.topicNameTextBox);
            this.Controls.Add(this.connectButton);
            this.Controls.Add(this.schemaRegistryUrlTextBox);
            this.Controls.Add(this.bootstrapServersTextBox);
            this.Controls.Add(this.schemaRegistryUrlLabel);
            this.Controls.Add(this.bootstrapServersLabel);
            this.Controls.Add(this.resultListBox);
            this.Controls.Add(this.consumeButton);
            this.Controls.Add(this.beginProduceButton);
            this.Controls.Add(this.produceAsyncButton);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.Fixed3D;
            this.Name = "MainWindow";
            this.Text = "Kafka Example";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button produceAsyncButton;
        private System.Windows.Forms.Button beginProduceButton;
        private System.Windows.Forms.Button consumeButton;
        private System.Windows.Forms.ListBox resultListBox;
        private System.Windows.Forms.Label bootstrapServersLabel;
        private System.Windows.Forms.Label schemaRegistryUrlLabel;
        private System.Windows.Forms.TextBox bootstrapServersTextBox;
        private System.Windows.Forms.TextBox schemaRegistryUrlTextBox;
        private System.Windows.Forms.Button connectButton;
        private System.Windows.Forms.TextBox topicNameTextBox;
        private System.Windows.Forms.Label topicNameLabel;
    }
}